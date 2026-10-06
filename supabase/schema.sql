-- ==========================================
-- 1. TABEL CONVERSATIONS
-- ==========================================
CREATE TABLE IF NOT EXISTS public.conversations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
  title TEXT NOT NULL DEFAULT 'Obrolan Baru',
  model TEXT NOT NULL DEFAULT 'gpt-4o-mini',
  created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

-- ==========================================
-- 2. TABEL MESSAGES
-- ==========================================
CREATE TABLE IF NOT EXISTS public.messages (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id UUID REFERENCES public.conversations(id) ON DELETE CASCADE,
  role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
  content TEXT NOT NULL DEFAULT '',
  thinking TEXT,
  image_url TEXT,
  created_at TIMESTAMPTZ DEFAULT now() NOT NULL
);

-- Index untuk performa
CREATE INDEX IF NOT EXISTS idx_conversations_user_id ON public.conversations(user_id);
CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON public.messages(conversation_id);

-- Trigger auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_conversations_updated_at ON public.conversations;
CREATE TRIGGER trg_conversations_updated_at
  BEFORE UPDATE ON public.conversations
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ==========================================
-- 3. ROW LEVEL SECURITY (RLS)
-- ==========================================
ALTER TABLE public.conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.messages ENABLE ROW LEVEL SECURITY;

-- Kebijakan untuk conversations
CREATE POLICY "Users can see own conversations"
  ON public.conversations FOR SELECT
  USING (auth.uid() = user_id);

CREATE POLICY "Users can insert own conversations"
  ON public.conversations FOR INSERT
  WITH CHECK (auth.uid() = user_id);

CREATE POLICY "Users can update own conversations"
  ON public.conversations FOR UPDATE
  USING (auth.uid() = user_id);

CREATE POLICY "Users can delete own conversations"
  ON public.conversations FOR DELETE
  USING (auth.uid() = user_id);

-- Kebijakan untuk messages (via conversation ownership)
CREATE POLICY "Users can see own messages"
  ON public.messages FOR SELECT
  USING (EXISTS (
    SELECT 1 FROM public.conversations
    WHERE conversations.id = messages.conversation_id
      AND conversations.user_id = auth.uid()
  ));

CREATE POLICY "Users can insert own messages"
  ON public.messages FOR INSERT
  WITH CHECK (EXISTS (
    SELECT 1 FROM public.conversations
    WHERE conversations.id = messages.conversation_id
      AND conversations.user_id = auth.uid()
  ));

CREATE POLICY "Users can update own messages"
  ON public.messages FOR UPDATE
  USING (EXISTS (
    SELECT 1 FROM public.conversations
    WHERE conversations.id = messages.conversation_id
      AND conversations.user_id = auth.uid()
  ));

CREATE POLICY "Users can delete own messages"
  ON public.messages FOR DELETE
  USING (EXISTS (
    SELECT 1 FROM public.conversations
    WHERE conversations.id = messages.conversation_id
      AND conversations.user_id = auth.uid()
  ));

-- ==========================================
-- 4. STORAGE BUCKETS
-- ==========================================
INSERT INTO storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
VALUES
  ('chat-uploads', 'chat-uploads', true, 5242880, ARRAY['image/png','image/jpeg','image/webp','image/gif']),
  ('ai-generated', 'ai-generated', true, 5242880, ARRAY['image/png','image/jpeg','image/webp'])
ON CONFLICT (id) DO NOTHING;

-- Storage RLS policies
CREATE POLICY "Public read uploads"
  ON storage.objects FOR SELECT
  USING (bucket_id = 'chat-uploads');

CREATE POLICY "Auth users upload"
  ON storage.objects FOR INSERT
  WITH CHECK (bucket_id IN ('chat-uploads','ai-generated') AND auth.role() = 'authenticated');

CREATE POLICY "Public read ai-generated"
  ON storage.objects FOR SELECT
  USING (bucket_id = 'ai-generated');
