import PostalMime from 'postal-mime';

export default {
  // 1. MANEJADOR HTTP: Gestión de voz y estado de llamadas
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // Ruta A: El comercial pulsa "1" para aceptar el lead
    if (request.method === 'POST' && url.pathname === '/gather') {
      const formData = await request.formData();
      const digits = formData.get('Digits'); 
      const telefonoCliente = url.searchParams.get('tel'); 
      const telefonoAgencia = url.searchParams.get('from'); 

      if (digits === '1') {
        const xmlDial = `<?xml version="1.0" encoding="UTF-8"?>
          <Response>
            <Dial callerId="${telefonoAgencia}">${telefonoCliente}</Dial>
          </Response>`;
        return new Response(xmlDial, { headers: { 'Content-Type': 'text/xml' } });
      }

      const xmlHangup = `<?xml version="1.0" encoding="UTF-8"?>
        <Response>
          <Say language="es-ES">Llamada cancelada.</Say>
          <Hangup/>
        </Response>`;
      return new Response(xmlHangup, { headers: { 'Content-Type': 'text/xml' } });
    }

    // Ruta B: Twilio nos avisa de que la llamada ha terminado (Status Callback)
    if (request.method === 'POST' && url.pathname === '/status') {
      const formData = await request.formData();
      const callStatus = formData.get('CallStatus'); // 'completed', 'busy', 'no-answer', 'failed'
      const callDuration = formData.get('CallDuration'); // en segundos
      const twilioSid = formData.get('CallSid');
      
      const orgId = url.searchParams.get('org_id');
      const agentId = url.searchParams.get('agent_id');
      const leadId = url.searchParams.get('lead_id');

      if (orgId && agentId) {
        const headersDb = {
          'apikey': env.SUPABASE_SERVICE_KEY,
          'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
          'Content-Type': 'application/json'
        };

        // Escribimos el historial en el búnker
        await fetch(`${env.SUPABASE_URL}/rest/v1/calls`, {
          method: 'POST',
          headers: { ...headersDb, 'Prefer': 'return=minimal' },
          body: JSON.stringify({
            org_id: orgId,
            agent_id: agentId,
            lead_id: leadId || null,
            twilio_sid: twilioSid,
            status: callStatus,
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
    let orgIdToLog = "desconocido"; 

    try {
      console.log(`Iniciando captura de lead en buzón: ${message.to}`);
      const email = await PostalMime.parse(message.raw, { attachmentEncoding: 'base64' });
      const leadNombre = email.from?.name || "Usuario Web";
      const leadEmail = email.from?.address || "Sin Email";
      const contenido = email.text || email.html || "";

      const phoneRegex = /(?:\+34|0034|34)?[\s\-]*(?:6|7)(?:[\s\-]*\d){8}/;
      const match = contenido.match(phoneRegex);
      if (!match) return; 
      const telefonoCliente = "+34" + match[0].replace(/\D/g, '').slice(-9);

      const headersDb = {
          'apikey': env.SUPABASE_SERVICE_KEY,
          'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
          'Content-Type': 'application/json'
      };

      // Extraemos los datos de configuración
      const orgRes = await fetch(`${env.SUPABASE_URL}/rest/v1/organizations?inbound_email=eq.${message.to}&select=id,business_hours,assigned_phone,ai_prompt_template`, { headers: headersDb });
      const orgData = await orgRes.json();

      if (!orgData || orgData.length === 0) return;

      const orgId = orgData[0].id;
      orgIdToLog = orgId; 
      const bizHours = orgData[0].business_hours || { open: "09:00", close: "21:00" };
      const telefonoAgencia = orgData[0].assigned_phone;
      const customPrompt = orgData[0].ai_prompt_template || "Resume este lead en 15 palabras. Indica nombre y motivo de contacto. Sin saludos ni instrucciones.";

      if (!telefonoAgencia) throw new Error("Agencia sin número de teléfono asignado.");

      const madridHourStr = new Date().toLocaleString("en-US", { timeZone: "Europe/Madrid", hour: '2-digit', hour12: false });
      const currentHour = Number(madridHourStr);
      const isOutOfHours = (currentHour < Number(bizHours.open.split(':')[0]) || currentHour >= Number(bizHours.close.split(':')[0]));

      const agentRes = await fetch(`${env.SUPABASE_URL}/rest/v1/agents?org_id=eq.${orgId}&is_receiving_calls=eq.true&consecutive_misses=lt.3&order=last_assigned_at.asc&limit=1`, { headers: headersDb });
      const agents = await agentRes.json();
      const agent = agents[0];
      if (!agent) return;

      let susurro = "Nuevo lead recibido.";
      if (!isOutOfHours) {
          const groqRes = await fetch("https://api.groq.com/openai/v1/chat/completions", {
              method: "POST",
              headers: { "Authorization": `Bearer ${env.GROQ_API_KEY}`, "Content-Type": "application/json" },
              body: JSON.stringify({
                  model: "llama-3.1-8b-instant",
                  messages: [
                      { role: "system", content: customPrompt },
                      { role: "user", content: `Lead de: ${leadNombre}. Texto: ${contenido.substring(0, 400)}` }
                  ],
                  temperature: 0
              })
          });
          const groqData = await groqRes.json();
          const aiText = groqData.choices?.[0]?.message?.content;
          if (aiText) susurro = aiText;
      }

      const leadInsert = await fetch(`${env.SUPABASE_URL}/rest/v1/leads`, {
          method: 'POST',
          headers: { ...headersDb, 'Prefer': 'return=representation' },
          body: JSON.stringify({
              org_id: orgId,
              source_channel: "portal_inmobiliario",
              parsed_data: { nombre: leadNombre, email: leadEmail, telefono: telefonoCliente },
              ai_whisper: susurro,
              assigned_agent_id: agent.id,
              status: isOutOfHours ? "out_of_hours" : "connected"
          })
      });
      const insertedLeads = await leadInsert.json();
      const newLeadId = insertedLeads[0]?.id;

      if (isOutOfHours) return;

      // Actualizamos turno del comercial
      await fetch(`${env.SUPABASE_URL}/rest/v1/agents?id=eq.${agent.id}`, {
          method: 'PATCH',
          headers: headersDb,
          body: JSON.stringify({ last_assigned_at: new Date().toISOString() })
      });

      const twilioAuth = btoa(`${env.TWILIO_ACCOUNT_SID}:${env.TWILIO_AUTH_TOKEN}`);
      const twiml = `<?xml version="1.0" encoding="UTF-8"?>
        <Response>
          <Gather numDigits="1" action="${env.WORKER_URL}/gather?tel=${encodeURIComponent(telefonoCliente)}&amp;from=${encodeURIComponent(telefonoAgencia)}" method="POST" timeout="10">
            <Pause length="1"/>
            <Say language="es-ES" voice="Polly.Lucia">Atención NeoVox. ${susurro} Para conectar la llamada, pulsa 1.</Say>
          </Gather>
          <Say language="es-ES">Tiempo de espera agotado.</Say>
          <Hangup/>
        </Response>`;

      // Montamos la URL de retorno para el historial
      const statusCallbackUrl = `${env.WORKER_URL}/status?org_id=${orgId}&agent_id=${agent.id}&lead_id=${newLeadId || ''}`;

      await fetch(`https://api.twilio.com/2010-04-01/Accounts/${env.TWILIO_ACCOUNT_SID}/Calls.json`, {
          method: 'POST',
          headers: { 'Authorization': `Basic ${twilioAuth}`, 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({ 
            To: agent.phone_number, 
            From: telefonoAgencia, 
            Twiml: twiml,
            StatusCallback: statusCallbackUrl,
            StatusCallbackEvent: 'completed',
            StatusCallbackMethod: 'POST'
          }).toString()
      });

      if (newLeadId) {
          await fetch(`${env.SUPABASE_URL}/rest/v1/interactions`, {
              method: 'POST',
              headers: { ...headersDb, 'Prefer': 'return=minimal' },
              body: JSON.stringify({ org_id: orgId, lead_id: newLeadId, agent_id: agent.id, connection_latency_ms: 0 })
          });
      }

    } catch (error) {
      console.log("Fallo crítico:", error.message);
      if (env.SUPABASE_URL && env.SUPABASE_SERVICE_KEY) {
          const headersDb = {
            'apikey': env.SUPABASE_SERVICE_KEY,
            'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
            'Content-Type': 'application/json'
          };
          await fetch(`${env.SUPABASE_URL}/rest/v1/leads`, {
              method: 'POST',
              headers: { ...headersDb, 'Prefer': 'return=minimal' },
              body: JSON.stringify({
                  org_id: orgIdToLog,
                  source_channel: "system_error",
                  parsed_data: { error_interno: error.message },
                  ai_whisper: "ERROR: " + error.message,
                  status: "failed"
              })
          });
      }
    }
  }
};