import React, { useState, useRef, useEffect } from 'react';
import { Send, Loader2 } from 'lucide-react';
import './ChatInput.css';

// 1. Define exactly what props this component expects
interface ChatInputProps {
  onSendMessage: (text: string) => void;
  isTyping: boolean;
}

const ChatInput: React.FC<ChatInputProps> = ({ onSendMessage, isTyping }) => {
  // 2. Type the state and the ref
  const [input, setInput] = useState<string>('');
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const adjustHeight = () => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = Math.min(textareaRef.current.scrollHeight, 150) + 'px';
    }
  };

  useEffect(() => {
    adjustHeight();
  }, [input]);

  // 3. Type the event parameters so TS knows what 'e' is
  const handleSubmit = (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    
    if (input.trim() && !isTyping) {
      onSendMessage(input.trim());
      setInput('');
      if (textareaRef.current) {
        textareaRef.current.style.height = 'auto';
      }
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(); // Trigger submit without the keyboard event
    }
  };

  return (
    <div className="chat-input-container">
      <form className="chat-input-form" onSubmit={handleSubmit}>
        <textarea
          ref={textareaRef}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Posez votre question sur les données agricoles..."
          className="chat-textarea"
          rows={1}
          disabled={isTyping}
        />
        <button 
          type="submit" 
          className="send-button"
          disabled={!input.trim() || isTyping}
        >
          {isTyping ? <Loader2 className="spinner" size={20} /> : <Send size={20} />}
        </button>
      </form>
      <div className="input-hint">
        L'IA agricole peut fournir des données historiques et des prévisions.
      </div>
    </div>
  );
};

export default ChatInput;