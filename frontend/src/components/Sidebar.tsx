import React, { useState, useEffect } from 'react';
import {
  Leaf,
  PlusCircle, 
  MapPin, 
  Maximize2, 
  Layers,
  Edit3,
  X,
  Save
} from 'lucide-react';
import { FarmProfile } from '../types';
import { GOVERNORATES, SOIL_TYPES, SIZE_UNITS } from '../constants/farmOptions';
import './Sidebar.css';

// 1. Strictly type the props
interface SidebarProps {
  isOpen: boolean;
  toggleSidebar: () => void;
  farmProfile: FarmProfile;
  setFarmProfile: (profile: FarmProfile) => void;
}

const Sidebar: React.FC<SidebarProps> = ({ isOpen, toggleSidebar, farmProfile, setFarmProfile }) => {
  const [isEditing, setIsEditing] = useState<boolean>(false);
  
  // 2. Local form state mirrors the LandingPage
  const [editFormData, setEditFormData] = useState({
    governorate: '',
    farm_size: '',
    farm_size_unit: 'Hectares',
    soil_type: ''
  });

  // 3. Clean string mapping - no more parsing "2.5 Hectares"
  useEffect(() => {
    if (isEditing && farmProfile) {
      setEditFormData({
        governorate: farmProfile.governorate || '',
        farm_size: farmProfile.farm_size ? farmProfile.farm_size.toString() : '',
        farm_size_unit: farmProfile.farm_size_unit || 'Hectares',
        soil_type: farmProfile.soil_type || ''
      });
    }
  }, [isEditing, farmProfile]);

  const handleInputChange = (e: React.ChangeEvent<HTMLSelectElement | HTMLInputElement>) => {
    const { name, value } = e.target;
    setEditFormData(prev => ({ ...prev, [name]: value }));
  };

  const handleSizeChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    let val = e.target.value.replace(',', '.');
    if (/^[0-9.]*$/.test(val)) {
      setEditFormData(prev => ({ ...prev, farm_size: val }));
    }
  };

  const handleSave = () => {
    if (!editFormData.governorate) {
      alert("Le gouvernorat est requis.");
      return;
    }

    // 4. Construct strict FarmProfile object
    const updatedProfile: FarmProfile = {
      governorate: editFormData.governorate,
      farm_size: editFormData.farm_size ? parseFloat(editFormData.farm_size) : null,
      farm_size_unit: editFormData.farm_size_unit || null,
      soil_type: editFormData.soil_type || null
    };

    setFarmProfile(updatedProfile);
    setIsEditing(false);
  };

  return (
    <>
      {isOpen && <div className="sidebar-overlay" onClick={toggleSidebar}></div>}
      
      <aside className={`sidebar ${isOpen ? 'open' : ''}`}>
        <div className="sidebar-header">
          <div className="logo-container">
            <Leaf className="logo-icon" size={28} />
            <h2>AgriAgent IA</h2>
          </div>
          {!isEditing && (
            <button className="new-chat-btn">
              <PlusCircle size={18} />
              <span>Nouveau Chat</span>
            </button>
          )}
        </div>

        <div className="sidebar-content">
          {isEditing ? (
            <div className="sidebar-edit-form animate-slide-up">
              <div className="edit-header">
                <h3>Modifier la Configuration</h3>
                <button onClick={() => setIsEditing(false)} className="cancel-icon-btn">
                  <X size={18} />
                </button>
              </div>

              <div className="form-group">
                <label><MapPin size={14} /> Gouvernorat</label>
                <select name="governorate" value={editFormData.governorate} onChange={handleInputChange}>
                  <option value="">Sélectionnez...</option>
                  {GOVERNORATES.map(gov => <option key={gov} value={gov}>{gov}</option>)}
                </select>
              </div>

              <div className="form-group">
                <label><Layers size={14} /> Type de Sol</label>
                <select name="soil_type" value={editFormData.soil_type} onChange={handleInputChange}>
                  <option value="">Sélectionnez...</option>
                  {SOIL_TYPES.map(soil => <option key={soil} value={soil}>{soil}</option>)}
                </select>
              </div>

              <div className="form-group">
                <label><Maximize2 size={14} /> Taille & Unité</label>
                <div className="composite-input">
                  <input 
                    type="text" 
                    value={editFormData.farm_size} 
                    onChange={handleSizeChange} 
                    placeholder="Taille" 
                  />
                  <select name="farm_size_unit" value={editFormData.farm_size_unit} onChange={handleInputChange}>
                    {SIZE_UNITS.map(unit => <option key={unit} value={unit}>{unit}</option>)}
                  </select>
                </div>
              </div>

              <button className="save-btn" onClick={handleSave}>
                <Save size={16} /> Enregistrer
              </button>
            </div>
          ) : (
            <>
              {farmProfile && (
                <div className="farm-info-card animate-slide-up">
                  <h3 className="section-title">Votre Exploitation</h3>
                  <div className="info-grid">
                    
                    {farmProfile.governorate && (
                      <div className="info-item">
                        <MapPin size={14} />
                        <span>{farmProfile.governorate}</span>
                      </div>
                    )}

                    {farmProfile.soil_type && (
                      <div className="info-item">
                        <Layers size={14} />
                        <span>{farmProfile.soil_type}</span>
                      </div>
                    )}

                    {farmProfile.farm_size && (
                      <div className="info-item">
                        <Maximize2 size={14} />
                        <span>{farmProfile.farm_size} {farmProfile.farm_size_unit}</span>
                      </div>
                    )}

                  </div>
                  <button className="edit-config-inline-btn" onClick={() => setIsEditing(true)}>
                    <Edit3 size={14} /> Modifier
                  </button>
                </div>
              )}
              
              {/* Note: The Chat History mockup block has been completely removed! */}
            </>
          )}
        </div>
      </aside>
    </>
  );
};

export default Sidebar;
