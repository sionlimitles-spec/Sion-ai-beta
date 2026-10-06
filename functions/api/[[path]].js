// functions/api/[[path]].js
// Cloudflare Pages Function — proxy AI API + streaming SSE + web search

export async function onRequest(context) {
  const { request, env } = context;
  const url = new URL(request.url);
  const path = url.pathname.replace('/api', '');

  // CORS preflight
  if (request.method === 'OPTIONS') {
    return new Response(null, {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      },
    });
  }

  // Routing
  if (path === '/chat' && request.method === 'POST') {
    return handleChat(request, env);
  }
  if (path === '/image' && request.method === 'POST') {
    return handleImage(request, env);
  }
  if (path === '/search' && request.method === 'POST') {
    return handleSearch(request, env);
  }
  if (path === '/models' && request.method === 'GET') {
    return handleModels(env);
  }
  if (path === '/upload' && request.method === 'POST') {
    return handleUpload(request, env);
  }

  return new Response(JSON.stringify({ error: 'Not found' }), {
    status: 404,
    headers: { 'Content-Type': 'application/json' },
  });
}

// =========================================================
// CHAT — Streaming SSE
// =========================================================
async function handleChat(request, env) {
  try {
    const body = await request.json();
    const {
      messages,
      model = 'gpt-4o-mini',
      temperature = 0.7,
      systemPrompt = '',
      thinkingMode = false,
      searchMode = false,
    } = body;

    // Jika search mode aktif, lakukan pencarian web dulu
    let searchContext = '';
    let sources = [];
    if (searchMode && env.TAVILY_API_KEY) {
      const lastUserMsg = messages.filter(m => m.role === 'user').pop();
      if (lastUserMsg) {
        const searchResult = await tavilySearch(lastUserMsg.content, env.TAVILY_API_KEY);
        if (searchResult) {
          searchContext = `\n\nKonteks dari pencarian web:\n${searchResult.answer}\n\nSumber:\n${searchResult.results.map(r => `- [${r.title}](${r.url})`).join('\n')}`;
          sources = searchResult.results.slice(0, 5).map(r => ({ title: r.title, url: r.url }));
        }
      }
    }

    // Bangun messages untuk AI
    const aiMessages = [];
    let finalSystemPrompt = systemPrompt || 'Kamu adalah asisten AI yang helpful dan ramah. Jawab dalam bahasa yang sama dengan pertanyaan pengguna.';

    if (thinkingMode) {
      finalSystemPrompt += '\n\nSebelum menjawab, tunjukkan proses berpikirmu di dalam tag <thinking>...</thinking>, lalu berikan jawaban final setelah tag tersebut ditutup.';
    }
    if (searchContext) {
      finalSystemPrompt += searchContext;
    }

    aiMessages.push({ role: 'system', content: finalSystemPrompt });
    for (const m of messages) {
      if (m.image_url) {
        // Multimodal: kirim sebagai content array
        aiMessages.push({
          role: m.role,
          content: [
            { type: 'text', text: m.content },
            { type: 'image_url', image_url: { url: m.image_url } },
          ],
        });
      } else {
        aiMessages.push({ role: m.role, content: m.content });
      }
    }

    // Pilih provider
    const provider = getProvider(model, env);
    if (!provider.apiKey) {
      return new Response(
        JSON.stringify({ error: `API key untuk provider "${provider.name}" belum dikonfigurasi.` }),
        { status: 500, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Panggil AI dengan streaming
    const aiRes = await fetch(`${provider.baseUrl}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${provider.apiKey}`,
      },
      body: JSON.stringify({
        model: provider.model,
        messages: aiMessages,
        stream: true,
        temperature,
        max_tokens: 4096,
      }),
    });

    if (!aiRes.ok) {
      const errText = await aiRes.text();
      return new Response(
        JSON.stringify({ error: `AI provider error (${aiRes.status}): ${errText.slice(0, 300)}` }),
        { status: aiRes.status, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Stream transform: parse SSE dari provider -> SSE ke client
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const encoder = new TextEncoder();

    (async () => {
      const reader = aiRes.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let fullContent = '';
      let thinkingContent = '';
      let inThinking = false;
      let thinkingDone = false;

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';

          for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed || !trimmed.startsWith('data: ')) continue;
            const data = trimmed.slice(6);
            if (data === '[DONE]') {
              // Kirim event selesai dengan metadata
              await writer.write(encoder.encode(`data: ${JSON.stringify({
                type: 'done',
                thinking: thinkingContent || null,
                sources: sources.length ? sources : null,
              })}\n\n`));
              continue;
            }

            try {
              const parsed = JSON.parse(data);
              const delta = parsed.choices?.[0]?.delta?.content;
              if (!delta) continue;

              fullContent += delta;

              // Deteksi tag <thinking> untuk memisahkan thinking & jawaban
              let processedDelta = delta;

              if (delta.includes('<thinking>')) {
                inThinking = true;
                thinkingDone = false;
                processedDelta = processedDelta.replace('<thinking>', '');
              }
              if (inThinking && delta.includes('</thinking>')) {
                inThinking = false;
                thinkingDone = true;
                const parts = processedDelta.split('</thinking>');
                thinkingContent += parts[0];
                processedDelta = parts[1] || '';
                if (thinkingContent) {
                  await writer.write(encoder.encode(`data: ${JSON.stringify({
                    type: 'thinking',
                    content: thinkingContent,
                    done: true,
                  })}\n\n`));
                }
              }

              if (inThinking) {
                thinkingContent += processedDelta;
                await writer.write(encoder.encode(`data: ${JSON.stringify({
                  type: 'thinking',
                  content: thinkingContent,
                  done: false,
                })}\n\n`));
              } else if (processedDelta) {
                await writer.write(encoder.encode(`data: ${JSON.stringify({
                  type: 'text',
                  content: processedDelta,
                })}\n\n`));
              }
            } catch {
              // skip malformed JSON
            }
          }
        }
      } catch (e) {
        try {
          await writer.write(encoder.encode(`data: ${JSON.stringify({ type: 'error', message: e.message })}\n\n`));
        } catch {}
      } finally {
        try { await writer.write(encoder.encode('data: [DONE]\n\n')); } catch {}
        try { await writer.close(); } catch {}
      }
    })();

    return new Response(readable, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
        'Access-Control-Allow-Origin': '*',
      },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}

// =========================================================
// IMAGE GENERATION
// =========================================================
async function handleImage(request, env) {
  try {
    const { prompt, size = '1024x1024' } = await request.json();
    if (!prompt) {
      return new Response(JSON.stringify({ error: 'Prompt diperlukan' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    if (!env.OPENAI_API_KEY) {
      return new Response(JSON.stringify({ error: 'OPENAI_API_KEY belum dikonfigurasi' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const res = await fetch('https://api.openai.com/v1/images/generations', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${env.OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: 'gpt-image-1',
        prompt,
        n: 1,
        size,
        quality: 'auto',
      }),
    });

    if (!res.ok) {
      const err = await res.text();
      return new Response(JSON.stringify({ error: `Image API error: ${err.slice(0, 300)}` }), {
        status: res.status,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const data = await res.json();
    const b64 = data.data?.[0]?.b64_json;
    if (!b64) {
      return new Response(JSON.stringify({ error: 'Gagal generate gambar' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    // Upload ke Supabase Storage
    let imageUrl = null;
    if (env.SUPABASE_URL && env.SUPABASE_ANON_KEY) {
      imageUrl = await uploadToSupabase(b64, prompt, env);
    }

    // Fallback: return base64
    if (!imageUrl) {
      imageUrl = `data:image/png;base64,${b64}`;
    }

    return new Response(JSON.stringify({ url: imageUrl, prompt }), {
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}

async function uploadToSupabase(b64, prompt, env) {
  try {
    const binaryStr = atob(b64);
    const bytes = new Uint8Array(binaryStr.length);
    for (let i = 0; i < binaryStr.length; i++) bytes[i] = binaryStr.charCodeAt(i);

    const filename = `gen_${Date.now()}_${Math.random().toString(36).slice(2, 8)}.png`;
    const uploadRes = await fetch(`${env.SUPABASE_URL}/storage/v1/object/ai-generated/${filename}`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
        'Content-Type': 'image/png',
      },
      body: bytes,
    });

    if (!uploadRes.ok) return null;
    return `${env.SUPABASE_URL}/storage/v1/object/public/ai-generated/${filename}`;
  } catch {
    return null;
  }
}

// =========================================================
// WEB SEARCH (Tavily)
// =========================================================
async function handleSearch(request, env) {
  try {
    const { query } = await request.json();
    if (!env.TAVILY_API_KEY) {
      return new Response(JSON.stringify({ error: 'TAVILY_API_KEY belum dikonfigurasi' }), {
        status: 500, headers: { 'Content-Type': 'application/json' },
      });
    }
    const result = await tavilySearch(query, env.TAVILY_API_KEY);
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), { status: 500 });
  }
}

async function tavilySearch(query, apiKey) {
  const res = await fetch('https://api.tavily.com/search', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      api_key: apiKey,
      query,
      search_depth: 'basic',
      include_answer: true,
      max_results: 5,
    }),
  });
  if (!res.ok) return null;
  const data = await res.json();
  return {
    answer: data.answer || '',
    results: (data.results || []).map(r => ({
      title: r.title,
      url: r.url,
      content: (r.content || '').slice(0, 300),
    })),
  };
}

// =========================================================
// UPLOAD GAMBAR
// =========================================================
async function handleUpload(request, env) {
  try {
    const formData = await request.formData();
    const file = formData.get('file');
    if (!file) {
      return new Response(JSON.stringify({ error: 'File diperlukan' }), { status: 400 });
    }

    const maxSize = 5 * 1024 * 1024; // 5MB
    if (file.size > maxSize) {
      return new Response(JSON.stringify({ error: 'Ukuran file maksimal 5MB' }), { status: 400 });
    }

    if (!env.SUPABASE_URL || !env.SUPABASE_ANON_KEY) {
      // Fallback: return data URL
      const buffer = await file.arrayBuffer();
      const b64 = btoa(String.fromCharCode(...new Uint8Array(buffer)));
      return new Response(JSON.stringify({
        url: `data:${file.type};base64,${b64}`,
      }), { headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
    }

    const ext = file.name.split('.').pop() || 'png';
    const filename = `upload_${Date.now()}_${Math.random().toString(36).slice(2, 8)}.${ext}`;

    const uploadRes = await fetch(`${env.SUPABASE_URL}/storage/v1/object/chat-uploads/${filename}`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${env.SUPABASE_ANON_KEY}`,
        'Content-Type': file.type,
      },
      body: await file.arrayBuffer(),
    });

    if (!uploadRes.ok) {
      const err = await uploadRes.text();
      return new Response(JSON.stringify({ error: `Upload gagal: ${err.slice(0, 200)}` }), { status: 500 });
    }

    const publicUrl = `${env.SUPABASE_URL}/storage/v1/object/public/chat-uploads/${filename}`;
    return new Response(JSON.stringify({ url: publicUrl }), {
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), { status: 500 });
  }
}

// =========================================================
// MODELS
// =========================================================
function handleModels(env) {
  const models = [
    { id: 'gpt-4o-mini', name: 'GPT-4o Mini', provider: 'openai' },
    { id: 'gpt-4o', name: 'GPT-4o', provider: 'openai' },
    { id: 'llama-3.3-70b-versatile', name: 'Llama 3.3 70B (Groq)', provider: 'groq' },
    { id: 'llama-3.1-8b-instant', name: 'Llama 3.1 8B (Groq)', provider: 'groq' },
    { id: 'qwen-qwq-32b', name: 'Qwen QWQ 32B (Groq)', provider: 'groq' },
  ];
  const available = models.filter(m => {
    if (m.provider === 'openai') return !!env.OPENAI_API_KEY;
    if (m.provider === 'groq') return !!env.GROQ_API_KEY;
    return false;
  });
  return new Response(JSON.stringify(available), {
    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
  });
}

// =========================================================
// PROVIDER RESOLVER
// =========================================================
function getProvider(model, env) {
  const groqModels = ['llama-3.3-70b-versatile', 'llama-3.1-8b-instant', 'qwen-qwq-32b', 'llama-3.2-90b-vision-preview'];
  if (groqModels.includes(model)) {
    return {
      name: 'Groq',
      apiKey: env.GROQ_API_KEY,
      baseUrl: 'https://api.groq.com/openai/v1',
      model,
    };
  }
  return {
    name: 'OpenAI',
    apiKey: env.OPENAI_API_KEY,
    baseUrl: env.OPENAI_BASE_URL || 'https://api.openai.com/v1',
    model,
  };
      }
