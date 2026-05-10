import React, { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Leaf, MapPin, Ruler, Sprout } from 'lucide-react';
import { FarmProfile } from '../types';
import { GOVERNORATES, SOIL_TYPES, SIZE_UNITS } from '../constants/farmOptions';
import './LandingPage.css';

export default function LandingPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const existingProfile = location.state?.farmProfile as FarmProfile | undefined;

  // 1. Keep form state as strings for easy text input management
  const [formData, setFormData] = useState({
    governorate: '',
    farm_size: '',
    farm_size_unit: 'Hectares',
    soil_type: ''
  });

  // 2. Initialize from existing profile if the user navigated back
  useEffect(() => {
    if (existingProfile) {
      setFormData({
        governorate: existingProfile.governorate || '',
        farm_size: existingProfile.farm_size ? existingProfile.farm_size.toString() : '',
        farm_size_unit: existingProfile.farm_size_unit || 'Hectares',
        soil_type: existingProfile.soil_type || ''
      });
    }
  }, [existingProfile]);

  // 3. Strongly typed event handlers
  const handleChange = (e: React.ChangeEvent<HTMLSelectElement | HTMLInputElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleSizeChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    let val = e.target.value;
    // Replace comma with dot for decimal inputs
    val = val.replace(',', '.');
    // Keep only numbers and dots
    if (/^[0-9.]*$/.test(val)) {
      setFormData(prev => ({
        ...prev,
        farm_size: val
      }));
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.governorate) {
      alert("Veuillez sélectionner un gouvernorat.");
      return;
    }

    // 4. Construct the strict TypeScript model to pass to the ChatPage
    const farmProfile: FarmProfile = {
      governorate: formData.governorate,
      farm_size: formData.farm_size ? parseFloat(formData.farm_size) : null,
      farm_size_unit: formData.farm_size_unit || null,
      soil_type: formData.soil_type || null
    };

    navigate('/chat', { state: { farmProfile } });
  };

  return (
    <div className="landing-container">
      <div className="landing-overlay" />
      <div className="onboarding-card animate-slide-up">
        <header className="onboarding-header">
          <div className="logo-wrapper">
            <Leaf size={32} className="logo-icon" />
          </div>
          <h1>Caractéristiques de votre Exploitation</h1>
          <p>Personnalisons l'assistant pour votre ferme en Tunisie.</p>
        </header>

        <form onSubmit={handleSubmit} className="onboarding-form">
          <div className="form-grid">
            
            <div className="form-group">
              <label>
                <MapPin size={18} /> Gouvernorat
              </label>
              <select 
                name="governorate" 
                value={formData.governorate} 
                onChange={handleChange}
                required
              >
                <option value="">Sélectionnez un gouvernorat</option>
                {GOVERNORATES.map(gov => (
                  <option key={gov} value={gov}>{gov}</option>
                ))}
              </select>
            </div>

            <div className="form-group">
              <label>
                <Sprout size={18} /> Type de Sol
              </label>
              <select 
                name="soil_type" 
                value={formData.soil_type} 
                onChange={handleChange}
              >
                <option value="">Sélectionnez le type de sol</option>
                {SOIL_TYPES.map(soil => (
                  <option key={soil} value={soil}>{soil}</option>
                ))}
              </select>
            </div>

            <div className="form-group">
              <label>
                <Ruler size={18} /> Taille (Optionnel)
              </label>
              <div className="composite-input">
                <input 
                  type="text" 
                  name="farm_size"
                  placeholder="ex: 2.5"
                  value={formData.farm_size} 
                  onChange={handleSizeChange}
                />
                <select 
                  name="farm_size_unit" 
                  value={formData.farm_size_unit} 
                  onChange={handleChange}
                >
                  {SIZE_UNITS.map(unit => (
                    <option key={unit} value={unit}>{unit}</option>
                  ))}
                </select>
              </div>
            </div>

          </div>

          <button type="submit" className="submit-btn">
            Commencer l'Analyse
          </button>
        </form>
      </div>
    </div>
  );
}
