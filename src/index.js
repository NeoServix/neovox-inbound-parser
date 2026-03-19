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

// Verificador criptográfico para la conexión segura con Stripe
async function verifyStripeSignature(payload, signatureHeader, secret) {
  if (!signatureHeader || !secret) return false;
  
  const parts = signatureHeader.split(',');
  let timestamp, signature;
  
  for (const part of parts) {
    if (part.startsWith('t=')) timestamp = part.substring(2);
    if (part.startsWith('v1=')) signature = part.substring(3);
  }
  
  if (!timestamp || !signature) return false;

  const enc = new TextEncoder();
  const key = await crypto.subtle.importKey(
    'raw', enc.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false, ['sign']
  );

  const signedPayload = `${timestamp}.${payload}`;
  const signatureBytes = await crypto.subtle.sign('HMAC', key, enc.encode(signedPayload));
  const signatureHex = Array.from(new Uint8Array(signatureBytes)).map(b => b.toString(16).padStart(2, '0')).join('');

  return signatureHex === signature;
}

// Motor de búsqueda con Doble Salto (Zona -> General)
async function getNextAvailableAgent(env, orgId, currentAgentId, leadPrefix) {
  const headersDb = getDbHeaders(env);
  const excludeFilter = currentAgentId ? `&id=neq.${currentAgentId}` : '';
  const baseFilter = `org_id=eq.${orgId}&is_receiving_calls=eq.true&consecutive_misses=lt.3${excludeFilter}&order=last_assigned_at.asc&limit=1`;

  if (leadPrefix) {
    const urlZona = `${env.SUPABASE_URL}/rest/v1/agents?${baseFilter}&assigned_prefixes=ilike.*${leadPrefix}*`;
    const resZona = await fetch(urlZona, { headers: headersDb });
    const agentsZona = await resZona.json();
    
    if (agentsZona && agentsZona.length > 0) {
      return agentsZona[0];
    }
  }

  const urlGeneral = `${env.SUPABASE_URL}/rest/v1/agents?${baseFilter}&or=(assigned_prefixes.is.null,assigned_prefixes.eq.)`;
  const resGeneral = await fetch(urlGeneral, { headers: headersDb });
  const agentsGeneral = await resGeneral.json();
  
  return (agentsGeneral && agentsGeneral.length > 0) ? agentsGeneral[0] : null;
}

// Ensamblador de Correos hacia el CRM del cliente con remitente dinámico
async function sendToCRM(env, orgData, infoLead, estado, gestion, duracionStr) {
  if (!orgData.crm_forwarding_email || !env.RESEND_API_KEY) return;

  const htmlOriginal = infoLead.raw_payload?.html || infoLead.raw_payload?.text || "<p>Contenido original no disponible</p>";
  const asuntoOriginal = infoLead.raw_payload?.subject || "Nuevo contacto inmobiliario";

  const remitenteDinamico = orgData.inbound_email ? `NeoVox Relay <${orgData.inbound_email}>` : "NeoVox Relay <alertas@neovox.app>";

  const cabecera = `
    <div style="background-color: #f8f9fa; border-left: 4px solid #00A8E8; padding: 15px; margin-bottom: 20px; font-family: sans-serif;">
      <h3 style="margin-top: 0; color: #333; font-size: 16px;">--- INFORME NEOVOX ---</h3>
      <p style="margin: 5px 0; font-size: 14px;"><strong>Estado:</strong> ${estado}</p>
      <p style="margin: 5px 0; font-size: 14px;"><strong>Gestión:</strong> ${gestion}</p>
      <p style="margin: 5px 0; font-size: 14px;"><strong>Tiempo de llamada:</strong> ${duracionStr}</p>
    </div>
    <hr style="border: 1px solid #eee; margin-bottom: 20px;" />
  `;

  await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { "Authorization": `Bearer ${env.RESEND_API_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      from: remitenteDinamico,
      to: orgData.crm_forwarding_email,
      subject: `[NeoVox] ${asuntoOriginal}`,
      html: cabecera + htmlOriginal
    })
  });
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
        Timeout: '20', 
        StatusCallback: statusCallbackUrl,
        StatusCallbackEvent: 'completed',
        StatusCallbackMethod: 'POST'
      }).toString()
  });
}

// Lógica central del relevo
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

  let infoLead = null;
  if (leadId) {
    const leadRes = await fetch(`${env.SUPABASE_URL}/rest/v1/leads?id=eq.${leadId}&select=parsed_data,ai_whisper,raw_payload`, { headers: headersDb });
    const leadData = await leadRes.json();
    infoLead = leadData[0];
  }

  const leadPrefix = infoLead?.parsed_data?.prefijo_enrutamiento || null;
  const nextAgent = await getNextAvailableAgent(env, orgId, currentAgentId, leadPrefix);

  if (nextAgent && infoLead) {
    const orgRes = await fetch(`${env.SUPABASE_URL}/rest/v1/organizations?id=eq.${orgId}&select=assigned_phone`, { headers: headersDb });
    const orgData = await orgRes.json();

    if (orgData[0]) {
      const telCliente = infoLead.parsed_data?.telefono;
      const telAgencia = orgData[0].assigned_phone;
      const susurro = infoLead.ai_whisper || "Nuevo lead reasignado.";
      
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
    const orgRes = await fetch(`${env.SUPABASE_URL}/rest/v1/organizations?id=eq.${orgId}&select=contact_email,name,crm_forwarding_email,inbound_email`, { headers: headersDb });
    const orgData = await orgRes.json();
    
    if (infoLead && orgData[0]) {
        await sendToCRM(env, orgData[0], infoLead, "🔴 No atendido (Se agotó la cadena de relevos)", "Pendiente de llamada manual", "0 segundos");
    }

    const contactEmail = orgData[0]?.contact_email;
    if (contactEmail && env.RESEND_API_KEY) {
      const nombreCli = infoLead?.parsed_data?.nombre || "Desconocido";
      const telCli = infoLead?.parsed_data?.telefono || "No disponible";
      const resumen = infoLead?.ai_whisper || "Sin resumen disponible";

      await fetch("https://api.resend.com/emails", {
        method: "POST",
        headers: { "Authorization": `Bearer ${env.RESEND_API_KEY}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          from: "NeoVox Alertas <alertas@neovox.app>",
          to: contactEmail,
          subject: `URGENTE: Lead sin atender - ${nombreCli}`,
          html: `
            <div style="font-family: sans-serif; color: #333;">
              <h2 style="color: #e63946;">Alerta Crítica de Relevo</h2>
              <p>Ningún agente ha podido atender la llamada tras recorrer la cadena de disponibilidad.</p>
              <hr />
              <p><strong>Cliente:</strong> ${nombreCli}</p>
              <p><strong>Teléfono:</strong> <a href="tel:${telCli}">${telCli}</a></p>
              <p><strong>Contexto IA:</strong> ${resumen}</p>
              <hr />
              <p style="font-size: 12px; color: #666;">Devuelve la llamada manualmente desde tu terminal para no perder el contacto.</p>
            </div>`
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
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const headersDb = getDbHeaders(env);

    // RUTAS DE PAGO STRIPE (Enrutador de comandos)
    if (request.method === 'POST' && url.pathname === '/webhook') {
      const signature = request.headers.get('Stripe-Signature');
      const rawBody = await request.text(); 

      try {
        const isValid = await verifyStripeSignature(rawBody, signature, env.STRIPE_WEBHOOK_SECRET);
        
        if (!isValid) {
          console.log("Bloqueo de seguridad: Firma de Stripe inválida.");
          return new Response('Firma inválida', { status: 400 });
        }

        const payload = JSON.parse(rawBody);
        
        // Alta de producto o plan
        if (payload.type === 'checkout.session.completed') {
          const session = payload.data.object;
          const rawRef = session.client_reference_id; // Ej: "UUID|extra" o "UUID|pro"

          if (rawRef && rawRef.includes('|')) {
            const [orgId, accion] = rawRef.split('|');

            if (accion === 'extra') {
              const orgRes = await fetch(`${env.SUPABASE_URL}/rest/v1/organizations?id=eq.${orgId}&select=extra_agents_quota`, { headers: headersDb });
              const orgData = await orgRes.json();

              if (orgData && orgData.length > 0) {
                const currentQuota = orgData[0].extra_agents_quota || 0;
                await fetch(`${env.SUPABASE_URL}/rest/v1/organizations?id=eq.${orgId}`, {
                  method: 'PATCH',
                  headers: headersDb,
                  body: JSON.stringify({ extra_agents_quota: currentQuota + 1 })
                });
                console.log(`[Stripe] Cuota ampliada para agencia: ${orgId}`);
              }
            } else if (accion === 'essential' || accion === 'pro' || accion === 'elite') {
              await fetch(`${env.SUPABASE_URL}/rest/v1/organizations?id=eq.${orgId}`, {
                method: 'PATCH',
                headers: headersDb,
                body: JSON.stringify({ plan_tier: accion })
              });
              console.log(`[Stripe] Plan actualizado a ${accion} para agencia: ${orgId}`);
            }
          }
        }

        // Suscripción cancelada
        if (payload.type === 'customer.subscription.deleted') {
          const subscription = payload.data.object;
          console.log(`[Stripe] Suscripción cancelada detectada: ${subscription.id}`);
        }

        return new Response(JSON.stringify({ received: true }), { status: 200 });
      } catch (err) {
        console.log("Fallo procesando Webhook Stripe:", err.message);
        return new Response(`Error interno`, { status: 500 });
      }
    }

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
            <Dial callerId="${telefonoAgencia}" timeout="60">${telefonoCliente}</Dial>
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
      } else if (callStatus === 'completed' && answeredBy !== 'machine_start') {
        ctx.waitUntil((async () => {
          const orgRes = await fetch(`${env.SUPABASE_URL}/rest/v1/organizations?id=eq.${orgId}&select=crm_forwarding_email,inbound_email`, { headers: headersDb });
          const orgData = await orgRes.json();
          const leadRes = await fetch(`${env.SUPABASE_URL}/rest/v1/leads?id=eq.${leadId}&select=raw_payload`, { headers: headersDb });
          const leadData = await leadRes.json();
          const agentRes = await fetch(`${env.SUPABASE_URL}/rest/v1/agents?id=eq.${agentId}&select=full_name`, { headers: headersDb });
          const agentData = await agentRes.json();
          
          if (orgData[0] && leadData[0]) {
             await sendToCRM(env, orgData[0], leadData[0], "🟢 Conectado", agentData[0]?.full_name || "Comercial", `${callDuration || 0} segundos`);
          }
        })());
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

  async email(message, env, ctx) {
    let orgDataRescate = null;
    let safePayloadRescate = null;

    try {
      const email = await PostalMime.parse(message.raw, { attachmentEncoding: 'base64' });
      
      const remitente = (email.from?.address || "").toLowerCase();
      if (remitente === "alertas@neovox.app" || remitente.endsWith("@neovox.app")) {
          return;
      }

      const contenido = email.text || email.html || "";

      const safePayload = {
          from: email.from,
          to: email.to,
          subject: email.subject,
          text: email.text,
          html: email.html
      };

      safePayloadRescate = safePayload;

      const headersDb = getDbHeaders(env);

      const targetEmail = (message.to || "").toLowerCase();
      const orgRes = await fetch(`${env.SUPABASE_URL}/rest/v1/organizations?inbound_email=eq.${targetEmail}&select=id,business_hours,assigned_phone,ai_prompt_template,contact_email,name,crm_forwarding_email,inbound_email`, { headers: headersDb });
      const orgData = await orgRes.json();

      if (!Array.isArray(orgData) || orgData.length === 0) {
          return;
      }

      orgDataRescate = orgData[0];
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
        "origen": "idealista o fotocasa o pisos.com o tecnocasa o habitaclia o desconocido",
        "es_publicidad": true o false (Pon true SOLAMENTE si es claramente un boletín, newsletter o aviso genérico de portal sin un cliente real detrás),
        "prefijo_enrutamiento": "Busca un código corto que termine en guion (ej. Z1-, ALQ-) en la referencia del anuncio. Si no hay, pon null",
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
        throw new Error("Fallo en lectura de JSON de Groq");
      }

      if (datosExtraidos.es_publicidad === true) {
          await fetch(`${env.SUPABASE_URL}/rest/v1/leads?id=eq.${currentLeadId}`, {
              method: 'PATCH',
              headers: headersDb,
              body: JSON.stringify({ 
                  portal_source: datosExtraidos.origen,
                  parsed_data: datosExtraidos,
                  status: "rejected_spam" 
              })
          });
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

          await sendToCRM(env, orgData[0], { raw_payload: safePayload }, "🟡 Sin Teléfono (Revisión Manual)", "No procesable", "0 segundos");

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
          const leadPrefix = datosExtraidos.prefijo_enrutamiento || null;
          agent = await getNextAvailableAgent(env, orgId, null, leadPrefix);
      }

      let finalStatus = "connected";
      if (isOutOfHours) finalStatus = "pending_notification";
      else if (!agent) finalStatus = "unanswered";

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

      if (isOutOfHours) {
          await sendToCRM(env, orgData[0], { raw_payload: safePayload }, "🔵 Fuera de horario", "Pendiente de llamada manual", "0 segundos");
          return;
      }
      
      if (!agent) {
          await executeFallback(env, orgId, null, currentLeadId);
          return;
      }

      await triggerTwilioCall(env, agent, datosExtraidos.telefono, telefonoAgencia, datosExtraidos.susurro_ia, orgId, currentLeadId);

      if (currentLeadId) {
          await fetch(`${env.SUPABASE_URL}/rest/v1/interactions`, {
              method: 'POST',
              headers: { ...headersDb, 'Prefer': 'return=minimal' },
              body: JSON.stringify({ org_id: orgId, lead_id: currentLeadId, agent_id: agent.id, connection_latency_ms: 0 })
          });
      }

    } catch (error) {
      if (orgDataRescate && orgDataRescate.crm_forwarding_email && safePayloadRescate) {
         await sendToCRM(env, orgDataRescate, { raw_payload: safePayloadRescate }, "⚫ Fallo de Servidor", "Enviado en bruto por seguridad", "0 segundos");
      }
    }
  },

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