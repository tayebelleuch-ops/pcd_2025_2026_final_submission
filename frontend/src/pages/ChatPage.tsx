import { useState, useEffect } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import Sidebar from '../components/Sidebar';
import ChatArea from '../components/ChatArea';
import { Menu } from 'lucide-react';
import { FarmProfile } from '../types'; // Import our new interface
import './ChatPage.css';

export default function ChatPage() {
  const [sidebarOpen, setSidebarOpen] = useState<boolean>(true);
  const location = useLocation();
  const navigate = useNavigate();

  // 1. Strongly type the local state using the FarmProfile interface
  const [currentFarmProfile, setCurrentFarmProfile] = useState<FarmProfile | null>(
    location.state?.farmProfile || null
  );

  // 2. Redirect to landing page if the user arrived without a profile
  useEffect(() => {
    if (!currentFarmProfile) {
      navigate('/');
    }
  }, [currentFarmProfile, navigate]);

  // NOTE: The massive 30-line "SYSTEM CONTEXT UPDATE" useEffect has been completely deleted!
  // We also deleted the systemResponse state.

  if (!currentFarmProfile) return null;

  return (
    <div className="app-container">
      <Sidebar 
        isOpen={sidebarOpen} 
        toggleSidebar={() => setSidebarOpen(!sidebarOpen)} 
        farmProfile={currentFarmProfile}
        setFarmProfile={setCurrentFarmProfile}
      />
      <main className={`main-content ${sidebarOpen ? 'sidebar-open' : 'sidebar-closed'}`}>
        <header className="main-header">
          <button 
            onClick={() => setSidebarOpen(!sidebarOpen)} 
            className="menu-btn" 
            aria-label="Toggle Sidebar"
          >
            <Menu size={24} />
          </button>
          <div className="header-title">Assistant Agricole IA</div>
          <div className="header-badge">
            {/* 3. Updated from location to governorate to match our new model */}
            {currentFarmProfile.governorate}
          </div>
        </header>
        
        {/* 4. Pass ONLY the farm profile. The systemMessage prop is gone! */}
        <ChatArea 
          farmProfile={currentFarmProfile} 
        />
      </main>
    </div>
  );
}