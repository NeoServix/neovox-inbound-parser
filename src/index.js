import PostalMime from 'postal-mime';

// ============================================================================
// FUNCIONES AUXILIARES Y HERRAMIENTAS
// ============================================================================

function getMadridDateTime() {
  const now = new Date();
  const madridDate = new Date(now.toLocaleString("en-US", { timeZone: "Europe/Madrid" }));
  const days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
  
  const dayName = days[madridDate.getDay()];
  const hour = madridDate.getHours().toString().padStart(2, '0');
  const minute = madridDate.getMinutes().toString().padStart(2, '0');
  
  return { 
    dayName, 
    timeStr: `${hour}:${minute}`, 
    hourNum: madridDate.getHours() 
  };
}

function getDbHeaders(env) {
  return {
    'apikey': env.SUPABASE_SERVICE_KEY,
    'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
    'Content-Type': 'application/json'
  };
}

// Dispara la llamada en Twilio y actualiza la hora del comercial
async function triggerTwilioCall(env, agent, telefonoCliente, telefonoAgencia, susurro, orgId, leadId) {
  const headersDb = getDbHeaders(env);

  await fetch(`${env.SUPABASE_URL}/rest/v1/agents?id=eq.${agent.id}`, {
      method: 'PATCH',
      headers: headersDb,
      body: JSON.stringify({ last_assigned_at: new Date().toISOString() })
  });

  const twilioAuth = btoa(`${env.TWILIO_ACCOUNT_SID}:${env.TWILIO_AUTH_TOKEN}`);
  
  const twiml = `<?xml version="1.0" encoding="UTF-8"?>
    <Response>
      <Gather numDigits="1" action="${env.WORKER_URL}/gather?tel=${encodeURIComponent(telefonoCliente)}&amp;from=${encodeURIComponent(telefonoAgencia)}&amp;agent_id=${agent.id}" method="POST" timeout="10">
        <Pause length="1"/>
        <Say language="es-ES" voice="Polly.Lucia">Atención NeoVox. ${susurro} Para conectar la llamada, pulsa 1.</Say>
      </Gather>
      <Redirect method="POST">${env.WORKER_URL}/fallback?org_id=${orgId}&amp;agent_id=${agent.id}&amp;lead_id=${leadId}</Redirect>
    </Response>`;

  const statusCallbackUrl = `${env.WORKER_URL}/status?org_id=${orgId}&agent_id=${agent.id}&lead_id=${leadId}`;

  await fetch(`https://api.twilio.com/2010-04-01/Accounts/${env.TWILIO_ACCOUNT_SID}/Calls.json`, {
      method: 'POST',
      headers: { 'Authorization': `Basic ${twilioAuth}`, 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ 
        To: agent.phone_number, 
        From: telefonoAgencia, 
        Twiml: twiml,
        MachineDetection: 'Enable', 
        StatusCallback: statusCallbackUrl,
        StatusCallbackEvent: 'completed',
        StatusCallbackMethod: 'POST'
      }).toString()
  });
}

// Lógica central del relevo: suma el fallo y busca al siguiente en la lista
async function executeFallback(env, orgId, currentAgentId, leadId) {
  const headersDb = getDbHeaders(env);

  if (currentAgentId) {
    const currentAgentRes = await fetch(`${env.SUPABASE_URL}/rest/v1/agents?id=eq.${currentAgentId}&select=consecutive_misses`, { headers: headersDb });
    const currentAgentData = await currentAgentRes.json();
    const currentMisses = currentAgentData[0]?.consecutive_misses || 0;
    
    await fetch(`${env.SUPABASE_URL}/rest/v1/agents?id=eq.${currentAgentId}`, {
      method: 'PATCH',
      headers: headersDb,
      body: JSON.stringify({ consecutive_misses: currentMisses + 1 })
    });
  }

  const agentRes = await fetch(`${env.SUPABASE_URL}/rest/v1/agents?org_id=eq.${orgId}&is_receiving_calls=eq.true&consecutive_misses=lt.3&id=neq.${currentAgentId}&order=last_assigned_at.asc&limit=1`, { headers: headersDb });
  const nextAgents = await agentRes.json();
  const nextAgent = nextAgents[0];

  if (nextAgent && leadId) {
    const leadRes = await fetch(`${env.SUPABASE_URL}/rest/v1/leads?id=eq.${leadId}&select=parsed_data,ai_whisper`, { headers: headersDb });
    const leadData = await leadRes.json();
    const orgRes = await fetch(`${env.SUPABASE_URL}/rest/v1/organizations?id=eq.${orgId}&select=assigned_phone`, { headers: headersDb });
    const orgData = await orgRes.json();

    if (leadData[0] && orgData[0]) {
      const telCliente = leadData[0].parsed_data?.telefono;
      const telAgencia = orgData[0].assigned_phone;
      const susurro = leadData[0].ai_whisper || "Nuevo lead reasignado.";
      
      await fetch(`${env.SUPABASE_URL}/rest/v1/leads?id=eq.${leadId}`, {
          method: 'PATCH',
          headers: headersDb,
          body: JSON.stringify({ assigned_agent_id: nextAgent.id })
      });

      await triggerTwilioCall(env, nextAgent, telCliente, telAgencia, susurro, orgId, leadId);

      await fetch(`${env.SUPABASE_URL}/rest/v1/interactions`, {
          method: 'POST',
          headers: { ...headersDb, 'Prefer': 'return=minimal' },
          body: JSON.stringify({ org_id: orgId, lead_id: leadId, agent_id: nextAgent.id, connection_latency_ms: 0 })
      });
    }
  } else {
    const orgRes = await fetch(`${env.SUPABASE_URL}/rest/v1/organizations?id=eq.${orgId}&select=contact_email,name`, { headers: headersDb });
    const orgData = await orgRes.json();
    const contactEmail = orgData[0]?.contact_email;

    if (contactEmail && env.RESEND_API_KEY) {
      await fetch("https://api.resend.com/emails", {
        method: "POST",
        headers: { "Authorization": `Bearer ${env.RESEND_API_KEY}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          from: "NeoVox Alertas <alertas@neovox.app>",
          to: contactEmail,
          subject: `Alerta Crítica: Lead sin atender en ${orgData[0]?.name}`,
          html: `<p>Ningún agente ha respondido a la llamada de conexión tras la cadena de relevo. Revisa el panel para contactar al cliente manualmente.</p>`
        })
      });
    }

    if (leadId) {
        await fetch(`${env.SUPABASE_URL}/rest/v1/leads?id=eq.${leadId}`, {
            method: 'PATCH',
            headers: headersDb,
            body: JSON.stringify({ status: 'unanswered' })
        });
    }
  }
}

// ============================================================================
// MOTOR PRINCIPAL (WORKER EXPORT)
// ============================================================================

export default {
  // 1. MANEJADOR HTTP: Gestión de voz y estado de llamadas
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const headersDb = getDbHeaders(env);

    if (request.method === 'POST' && url.pathname === '/gather') {
      const formData = await request.formData();
      const digits = formData.get('Digits'); 
      const telefonoCliente = url.searchParams.get('tel'); 
      const telefonoAgencia = url.searchParams.get('from');
      const agentId = url.searchParams.get('agent_id');

      if (digits === '1') {
        if (agentId) {
          ctx.waitUntil(fetch(`${env.SUPABASE_URL}/rest/v1/agents?id=eq.${agentId}`, {
            method: 'PATCH',
            headers: headersDb,
            body: JSON.stringify({ consecutive_misses: 0 })
          }));
        }

        const xmlDial = `<?xml version="1.0" encoding="UTF-8"?>
          <Response>
            <Dial callerId="${telefonoAgencia}">${telefonoCliente}</Dial>
          </Response>`;
        return new Response(xmlDial, { headers: { 'Content-Type': 'text/xml' } });
      }

      const xmlRedirect = `<?xml version="1.0" encoding="UTF-8"?>
        <Response>
          <Redirect method="POST">${env.WORKER_URL}/fallback${url.search}</Redirect>
        </Response>`;
      return new Response(xmlRedirect, { headers: { 'Content-Type': 'text/xml' } });
    }

    if (request.method === 'POST' && url.pathname === '/fallback') {
      const orgId = url.searchParams.get('org_id');
      const agentId = url.searchParams.get('agent_id');
      const leadId = url.searchParams.get('lead_id');

      ctx.waitUntil(executeFallback(env, orgId, agentId, leadId));
      
      const xmlHangup = `<?xml version="1.0" encoding="UTF-8"?>
        <Response>
          <Say language="es-ES">Reasignando comercial.</Say>
          <Hangup/>
        </Response>`;
      return new Response(xmlHangup, { headers: { 'Content-Type': 'text/xml' } });
    }

    if (request.method === 'POST' && url.pathname === '/status') {
      const formData = await request.formData();
      const callStatus = formData.get('CallStatus'); 
      const callDuration = formData.get('CallDuration'); 
      const twilioSid = formData.get('CallSid');
      const answeredBy = formData.get('AnsweredBy'); 
      
      const orgId = url.searchParams.get('org_id');
      const agentId = url.searchParams.get('agent_id');
      const leadId = url.searchParams.get('lead_id');

      if (callStatus === 'no-answer' || callStatus === 'failed' || callStatus === 'busy' || answeredBy === 'machine_start') {
        ctx.waitUntil(executeFallback(env, orgId, agentId, leadId));
      }

      if (orgId && agentId) {
        await fetch(`${env.SUPABASE_URL}/rest/v1/calls`, {
          method: 'POST',
          headers: { ...headersDb, 'Prefer': 'return=minimal' },
          body: JSON.stringify({
            org_id: orgId,
            agent_id: agentId,
            lead_id: leadId || null,
            twilio_sid: twilioSid,
            status: answeredBy === 'machine_start' ? 'voicemail' : callStatus,
            duration: callDuration ? parseInt(callDuration, 10) : 0
          })
        });
      }

      return new Response('Estado guardado', { status: 200 });
    }

    return new Response('NeoVox Búnker Online', { status: 200 });
  },

  // 2. MANEJADOR DE EMAIL: Procesador de leads entrantes
  async email(message, env, ctx) {
    try {
      const email = await PostalMime.parse(message.raw, { attachmentEncoding: 'base64' });
      const contenido = email.text || email.html || "";

      const safePayload = {
          from: email.from,
          to: email.to,
          subject: email.subject,
          text: email.text,
          html: email.html
      };

      const headersDb = getDbHeaders(env);

      const targetEmail = (message.to || "").toLowerCase();
      const orgRes = await fetch(`${env.SUPABASE_URL}/rest/v1/organizations?inbound_email=eq.${targetEmail}&select=id,business_hours,assigned_phone,ai_prompt_template,contact_email,name`, { headers: headersDb });
      const orgData = await orgRes.json();

      if (!Array.isArray(orgData) || orgData.length === 0) {
          console.log("Rechazado: Agencia no encontrada o error DB para el correo", targetEmail);
          return;
      }

      const orgId = orgData[0].id;

      const leadInsert = await fetch(`${env.SUPABASE_URL}/rest/v1/leads`, {
          method: 'POST',
          headers: { ...headersDb, 'Prefer': 'return=representation' },
          body: JSON.stringify({
              org_id: orgId,
              source_channel: "correo_directo",
              raw_payload: safePayload, 
              status: "processing"
          })
      });
      
      const insertedLeads = await leadInsert.json();

      if (!Array.isArray(insertedLeads) || insertedLeads.length === 0) return;
      
      const currentLeadId = insertedLeads[0]?.id;
      const reglaAgencia = orgData[0].ai_prompt_template || "Resume el contacto entrante indicando el nombre y el inmueble en máximo 15 palabras. Cero saludos y cero despedidas.";

      const extractorPrompt = `Eres un procesador de datos backend. Analiza este correo inmobiliario y extrae la información del inquilino/comprador. Devuelve ÚNICAMENTE un objeto JSON válido con esta estructura exacta. Si un dato no aparece, pon null.
      {
        "origen": "idealista o fotocasa o pisos.com o habitaclia o desconocido",
        "nombre": "nombre del cliente",
        "telefono": "teléfono con prefijo internacional, ej: +34600000000",
        "perfil_inquilino": {
          "inmueble": "referencia o dirección del inmueble",
          "personas": "número de personas",
          "mascotas": "información sobre mascotas",
          "mudanza": "fecha de mudanza deseada",
          "ingresos": "datos económicos o laborales",
          "mensaje_original": "texto o carta de presentación del cliente"
        },
        "susurro_ia": "AQUÍ DEBES APLICAR ESTA REGLA ESTRICTA DE LA AGENCIA: ${reglaAgencia}"
      }`;

      const groqRes = await fetch("https://api.groq.com/openai/v1/chat/completions", {
          method: "POST",
          headers: { "Authorization": `Bearer ${env.GROQ_API_KEY}`, "Content-Type": "application/json" },
          body: JSON.stringify({
              model: "llama-3.1-8b-instant",
              response_format: { type: "json_object" },
              messages: [
                  { role: "system", content: extractorPrompt },
                  { role: "user", content: `Remitente: ${email.from?.address}. Asunto: ${email.subject}. Contenido: ${contenido.substring(0, 3000)}` }
              ],
              temperature: 0
          })
      });
      
      const groqData = await groqRes.json();
      const aiResponse = groqData.choices?.[0]?.message?.content;
      
      let datosExtraidos;
      try {
        datosExtraidos = JSON.parse(aiResponse);
      } catch (e) {
        console.log("Error al leer el JSON de la IA:", aiResponse);
        return;
      }

      if (!datosExtraidos.telefono) {
          await fetch(`${env.SUPABASE_URL}/rest/v1/leads?id=eq.${currentLeadId}`, {
              method: 'PATCH',
              headers: headersDb,
              body: JSON.stringify({ 
                  portal_source: datosExtraidos.origen,
                  parsed_data: datosExtraidos,
                  status: "manual_review_needed" 
              })
          });

          const contactEmail = orgData[0]?.contact_email;
          if (contactEmail && env.RESEND_API_KEY) {
            await fetch("https://api.resend.com/emails", {
              method: "POST",
              headers: { "Authorization": `Bearer ${env.RESEND_API_KEY}`, "Content-Type": "application/json" },
              body: JSON.stringify({
                from: "NeoVox Alertas <alertas@neovox.app>",
                to: contactEmail,
                subject: `Revisión Manual: Lead sin teléfono en ${orgData[0]?.name}`,
                html: `<p>Ha entrado un nuevo lead desde <strong>${datosExtraidos.origen || 'un portal'}</strong> para el inmueble <strong>${datosExtraidos.perfil_inquilino?.inmueble || 'desconocido'}</strong>, pero no se ha encontrado un número de teléfono válido en el correo.</p><p>El lead se ha guardado en tu panel para revisión manual.</p>`
              })
            });
          }
          return; 
      }

      const telefonoAgencia = orgData[0].assigned_phone;
      if (!telefonoAgencia) throw new Error("Agencia sin número asignado.");

      const { dayName, timeStr } = getMadridDateTime();
      const dbSchedule = orgData[0].business_hours || {};
      const todayConfig = dbSchedule[dayName] || { isOpen: true, open: "09:00", close: "21:00" };
      
      let isOutOfHours = true;
      if (todayConfig.isOpen) {
        if (timeStr >= todayConfig.open && timeStr < todayConfig.close) {
          isOutOfHours = false;
        }
      }

      let agent = null;
      if (!isOutOfHours) {
          const agentRes = await fetch(`${env.SUPABASE_URL}/rest/v1/agents?org_id=eq.${orgId}&is_receiving_calls=eq.true&consecutive_misses=lt.3&order=last_assigned_at.asc&limit=1`, { headers: headersDb });
          const agents = await agentRes.json();
          agent = agents[0];
      }

      let finalStatus = "connected";
      if (isOutOfHours) finalStatus = "pending_notification";
      else if (!agent) finalStatus = "unanswered";

      // Lógica de corrección de canal
      const canalReal = (datosExtraidos.origen && datosExtraidos.origen !== 'desconocido') 
          ? 'portal_inmobiliario' 
          : 'correo_directo';

      const updatePayload = { 
          source_channel: canalReal,
          portal_source: datosExtraidos.origen,
          parsed_data: datosExtraidos,
          ai_whisper: datosExtraidos.susurro_ia,
          status: finalStatus
      };

      // Aquí cerramos la asignación en una única escritura
      if (agent) {
          updatePayload.assigned_agent_id = agent.id;
      }

      const patchResFull = await fetch(`${env.SUPABASE_URL}/rest/v1/leads?id=eq.${currentLeadId}`, {
          method: 'PATCH',
          headers: headersDb,
          body: JSON.stringify(updatePayload)
      });

      if (!patchResFull.ok) {
          console.log("Error DB actualización completa:", await patchResFull.text());
      }

      if (isOutOfHours || !agent) return;

      await triggerTwilioCall(env, agent, datosExtraidos.telefono, telefonoAgencia, datosExtraidos.susurro_ia, orgId, currentLeadId);

      if (currentLeadId) {
          await fetch(`${env.SUPABASE_URL}/rest/v1/interactions`, {
              method: 'POST',
              headers: { ...headersDb, 'Prefer': 'return=minimal' },
              body: JSON.stringify({ org_id: orgId, lead_id: currentLeadId, agent_id: agent.id, connection_latency_ms: 0 })
          });
      }

    } catch (error) {
      console.log("Fallo crítico general:", error.message);
    }
  },

  // 3. MANEJADOR DE TAREAS: Disparador matutino de resúmenes
  async scheduled(event, env, ctx) {
    const { dayName, hourNum } = getMadridDateTime();
    const headersDb = getDbHeaders(env);

    const orgsRes = await fetch(`${env.SUPABASE_URL}/rest/v1/organizations?select=id,name,contact_email,business_hours,leads(id,parsed_data,ai_whisper)&leads.status=eq.pending_notification`, { headers: headersDb });
    const orgs = await orgsRes.json();

    const leadsToUpdate = [];

    for (const org of orgs) {
      if (!org.leads || org.leads.length === 0) continue;

      const dbSchedule = org.business_hours || {};
      const todayConfig = dbSchedule[dayName];

      if (!todayConfig || !todayConfig.isOpen) continue;

      const openHour = Number(todayConfig.open.split(':')[0]);

      if (openHour === hourNum) {
          const pendingLeads = org.leads;

          const leadsHtml = pendingLeads.map(l => 
            `<li style="margin-bottom: 12px; padding-bottom: 12px; border-bottom: 1px solid #eee;">
               <strong>Cliente:</strong> ${l.parsed_data?.nombre || 'Desconocido'}<br/>
               <strong>Teléfono:</strong> ${l.parsed_data?.telefono || 'Desconocido'}<br/>
               <strong>Resumen:</strong> ${l.ai_whisper || 'Sin resumen'}
             </li>`
          ).join('');

          const emailBody = `<h2>Resumen Matutino NeoVox</h2>
                             <p>Estos contactos entraron fuera de horario y están pendientes de llamada:</p>
                             <ul style="list-style: none; padding: 0;">${leadsHtml}</ul>`;

          if (org.contact_email && env.RESEND_API_KEY) {
            await fetch("https://api.resend.com/emails", {
              method: "POST",
              headers: { "Authorization": `Bearer ${env.RESEND_API_KEY}`, "Content-Type": "application/json" },
              body: JSON.stringify({
                from: "NeoVox Alertas <alertas@neovox.app>",
                to: org.contact_email,
                subject: `Cierre nocturno: ${pendingLeads.length} nuevos leads para ${org.name}`,
                html: emailBody
              })
            });

            pendingLeads.forEach(l => leadsToUpdate.push(l.id));
          }
      }
    }

    if (leadsToUpdate.length > 0) {
        const leadIds = leadsToUpdate.join(',');
        await fetch(`${env.SUPABASE_URL}/rest/v1/leads?id=in.(${leadIds})`, {
            method: 'PATCH',
            headers: headersDb,
            body: JSON.stringify({ status: 'notified' })
        });
    }
  }
};