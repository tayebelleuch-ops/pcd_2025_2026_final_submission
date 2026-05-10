import axios from 'axios';
import { ChatRequestPayload } from '../types';
// Make sure this points to your FastAPI server
const API_BASE_URL = "http://localhost:8001/api/v1";
// Define what the frontend expects back from the API
interface ApiResponse {
  answer: string;
  chart_data: any | null; // Replace 'any' with your chart interface later
}
const api = axios.create({
  // Make sure this is 8001 since that's your public Docker port!
  baseURL: API_BASE_URL, 
  timeout: 10000, // 10 seconds timeout
  headers: {
    'Content-Type': 'application/json',
  },
});
export const agentApi = {
  /**
   * Send a query to the agricultural AI agent.
   * Now uses the ChatRequestPayload interface for strict type safety.
   */
  async queryAgent(payload: ChatRequestPayload, useMock = false): Promise<ApiResponse> {
    if (useMock) {
      return this._getMockResponse(payload.message);
    }

    try {
      // The payload already contains message, governorate, farm_size, etc.
      const response = await axios.post(`${API_BASE_URL}/chat`, payload);

      return {
        answer: response.data.response,
        chart_data: response.data.chart_data
      };
    } catch (error) {
      console.warn("Backend unavailable, falling back to mock response.", error);
      return this._getMockResponse(payload.message);
    }
  },

  async _getMockResponse(question: string): Promise<ApiResponse> {
    return new Promise((resolve) => {
      setTimeout(() => {
        const isChartQuery = question.toLowerCase().includes('chart') ||
          question.toLowerCase().includes('prix') ||
          question.toLowerCase().includes('météo') ||
          Math.random() > 0.5;

        if (isChartQuery) {
          resolve({
            answer: `Voici les données simulées pour "${question}".`,
            chart_data: {
              type: 'bar',
              title: 'Données Simulées',
              mockData: true
            }
          });
        } else {
          resolve({
            answer: `Réponse texte simple pour : "${question}".`,
            chart_data: null
          });
        }
      }, 1200);
    });
  }
};