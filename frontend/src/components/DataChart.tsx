import React from 'react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts';
import { BarChart3 } from 'lucide-react';
import './ChartPlaceholder.css'; 

// 1. Define the exact structure Gemini sends back for charts!
export interface ChartPayload {
  location: string;
  dataset: Record<string, string | number>[]; 
  requested_metrics: string[];
}

interface DataChartProps {
  data: ChartPayload | null;
}

const CHART_COLORS = ["#10b981", "#3b82f6", "#f59e0b", "#ef4444"];

const DataChart: React.FC<DataChartProps> = ({ data }) => {
  if (!data || !data.dataset) return null;

  return (
    <div className="chart-placeholder animate-slide-up" style={{ padding: '1rem', background: 'white', borderRadius: '12px', marginTop: '10px', boxShadow: '0 2px 8px rgba(0,0,0,0.05)' }}>
      <div className="chart-header" style={{ marginBottom: '1rem', display: 'flex', alignItems: 'center', gap: '8px' }}>
        <BarChart3 size={20} color="#10b981" />
        <h4 style={{ margin: 0, color: '#333' }}>
          Analytiques : {data.location}
        </h4>
      </div>

      <div style={{ width: '100%', height: 300 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={data.dataset}
            margin={{ top: 5, right: 20, left: 0, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e5e7eb" />
            <XAxis
              dataKey="day"
              tick={{ fontSize: 12, fill: '#6b7280' }}
              tickLine={false}
              axisLine={false}
            />
            <YAxis
              tick={{ fontSize: 12, fill: '#6b7280' }}
              tickLine={false}
              axisLine={false}
            />
            <Tooltip
              contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px rgba(0,0,0,0.1)' }}
            />
            <Legend wrapperStyle={{ paddingTop: '10px' }} />

            {data.requested_metrics.map((metric, index) => (
              <Line
                key={metric}
                type="monotone"
                dataKey={metric}
                name={metric.charAt(0).toUpperCase() + metric.slice(1)} 
                stroke={CHART_COLORS[index % CHART_COLORS.length]}
                strokeWidth={3}
                dot={{ r: 4, strokeWidth: 2 }}
                activeDot={{ r: 6 }}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
};

export default DataChart;