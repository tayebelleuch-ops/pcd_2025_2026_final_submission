import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkBreaks from 'remark-breaks'; // <-- Add this import
import DataChart, { ChartPayload } from './DataChart'; 
import { User, Leaf } from 'lucide-react';
import './ChatMessage.css';

export interface MessagePayload {
  id: number;
  sender: 'user' | 'agent' | 'system';
  text: string;
  chartData?: ChartPayload | null; 
}

interface ChatMessageProps {
  message: MessagePayload;
}

const ChatMessage: React.FC<ChatMessageProps> = ({ message }) => {
  const isUser = message.sender === 'user';

  return (
    <div className={`message-wrapper ${isUser ? 'user' : 'agent'}`}>
      <div className="message-avatar">
        {isUser ? <User size={20} /> : <Leaf size={20} />}
      </div>

      <div className="message-content-area">
        <div className="message-bubble">
          <div className="message-text">
            {/* Add remarkPlugins={[remarkBreaks]} here */}
            <ReactMarkdown remarkPlugins={[remarkBreaks]}>
              {message.text}
            </ReactMarkdown>
          </div>
        </div>

        {!isUser && message.chartData && (
          <DataChart data={message.chartData} />
        )}
      </div>
    </div>
  );
};

export default ChatMessage;