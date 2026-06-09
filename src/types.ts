export interface ImageAttachment {
  id: string;              // Image ID — comes from the SSE imageId payload
  storageKey: string;      // IndexedDB key: `${conversationId}/${imageId}`
  url: string;             // Runtime blob: URL, render-only, not persisted
  mimeType: string;
  size: number;
  createdAt: number;
  persistent: boolean;     // True once the blob has been written to IndexedDB
}

export interface ImageSsePayload {
  imageId: string;
  base64: string;
  mimeType?: string;
  size?: number;
}

export interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: number;
  images?: (ImageAttachment | string)[];
  activity?: {
    type: 'web_search';
    label: string;
    status: 'active' | 'done' | 'error';
    /**
     * Optional in-memory error code that drives the in-bubble chip's CTA.
     * Currently only `wsa_missing` is recognised — emitted by the frontend
     * detector in App.tsx when a debug_msg surfaces a WSA_API_KEY-missing
     * tool error. Not persisted to /history; cleared the next time the
     * activity transitions back to `active`.
     */
    errorCode?: 'wsa_missing';
  };
  /**
   * True while the assistant is actively producing this message
   * (between the first text_delta and the final done/error event).
   * Drives the in-bubble blinking caret to give the user feedback
   * that more content is still streaming. Cleared once done/error fires.
   */
  streaming?: boolean;
}

export interface ToolLampState {
  id: string;
  label: string;
  icon: string;
  active: boolean;
  animKey: number;   // Increments on each activation so the animation element re-mounts and the animation replays.
}

/**
 * Lightweight summary of a conversation, returned by /conversations.
 * Used to render the left sidebar — does NOT contain full message content.
 */
export interface ConversationSummary {
  id: string;
  title: string;
  preview?: string;
  lastMessageAt?: number;
  createdAt?: number;
  userId?: string;
  messageCount?: number;
}

export interface ListConversationsParams {
  userId: string;
  limit?: number;
  order?: 'asc' | 'desc';
  after?: string;
  before?: string;
}

export interface ListConversationsResponse {
  conversations: ConversationSummary[];
  nextCursor?: string;
  previousCursor?: string;
}
