import React, { useState, useRef, useEffect } from 'react';
import ChatMessage from './ChatMessage';
import ChatInput from './ChatInput';
import { agentApi } from '../services/api';
import { Leaf } from 'lucide-react';
import { FarmProfile, ChatRequestPayload } from '../types';
import './ChatArea.css';

// 1. Define the props for this component using our new interface
interface ChatAreaProps {
  farmProfile: FarmProfile;
}

// 2. Define what a Message looks like in state
interface Message {
  id: number;
  sender: 'user' | 'agent' | 'system';
  text: string;
  chartData?: any; // You can strongly type your charts later!
}

const ChatArea: React.FC<ChatAreaProps> = ({ farmProfile }) => {
  // 3. Apply types to your React state and refs
  const [messages, setMessages] = useState<Message[]>([]);
  const [isTyping, setIsTyping] = useState<boolean>(false);
  const [conversationId] = useState<string>(
    () => globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(16).slice(2)}`
  );
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, isTyping]);

  const handleSendMessage = async (text: string) => {
    const userMsg: Message = { id: Date.now(), sender: 'user', text };
    setMessages((prev) => [...prev, userMsg]);
    setIsTyping(true);

    try {
      // 4. Construct the payload using the strict TS interface.
      // This seamlessly merges the user's message with the current farm profile.
      const payload: ChatRequestPayload = {
        conversation_id: conversationId,
        message: text,
        governorate: farmProfile?.governorate || null,
        farm_size: farmProfile?.farm_size || null,
        farm_size_unit: farmProfile?.farm_size_unit || null,
        soil_type: farmProfile?.soil_type || null
      };
      
      const response = await agentApi.queryAgent(payload);
      
      const agentMsg: Message = {
        id: Date.now() + 1,
        sender: 'agent',
        text: response.answer,
        chartData: response.chart_data
      };

      setMessages((prev) => [...prev, agentMsg]);
    } catch (error) {
      console.error("API Error:", error);
    } finally {
      setIsTyping(false);
    }
  };

  return (
    <div className="chat-area">
      <div className="messages-container">
        {messages.length === 0 && !isTyping ? (
          <div className="empty-state">
            <div className="empty-icon-wrapper">
              <Leaf size={48} className="empty-icon text-primary" />
            </div>
            <h3>Comment puis-je vous aider ?</h3>
            <p>Demandez des données sur les rendements, la météo ou les prix du marché.</p>
          </div>
        ) : (
          <div className="messages-list">
            {messages.map((msg) => (
              <ChatMessage key={msg.id} message={msg} />
            ))}
            {isTyping && (
              <div className="typing-indicator animate-pulse">
                <Leaf size={16} className="typing-icon" />
                <span>L'IA analyse vos données...</span>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
        )}
      </div>
      <ChatInput onSendMessage={handleSendMessage} isTyping={isTyping} />
    </div>
  );
};

export default ChatArea;
