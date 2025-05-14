import * as http from 'http';
import { metrics } from './metrics';

/**
 * HealthServer - Simple HTTP server for exposing health and metrics
 */
export class HealthServer {
  private server: http.Server | null = null;
  
  /**
   * Constructor
   * @param port Port to listen on (default: 8093)
   */
  constructor(private port: number = 8093) {}
  
  /**
   * Start the health server
   */
  public start(): void {
    if (this.server) {
      return; // Server already running
    }
    
    this.server = http.createServer((req, res) => {
      // Set CORS headers for all responses
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
      
      // Handle OPTIONS requests for CORS preflight
      if (req.method === 'OPTIONS') {
        res.statusCode = 204;
        res.end();
        return;
      }
      
      // Only handle GET requests
      if (req.method !== 'GET') {
        res.statusCode = 405;
        res.end('Method Not Allowed');
        return;
      }
      
      // Route handling
      const url = req.url || '/';
      
      if (url === '/health' || url === '/') {
        this.handleHealthCheck(req, res);
      } else if (url === '/metrics') {
        this.handleMetrics(req, res);
      } else {
        res.statusCode = 404;
        res.end('Not Found');
      }
    });
    
    this.server.listen(this.port, () => {
      console.log(`Health server listening on port ${this.port}`);
    });
    
    // Handle server errors
    this.server.on('error', (error) => {
      console.error(`Health server error: ${error}`);
    });
  }
  
  /**
   * Stop the health server
   */
  public stop(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.server) {
        resolve();
        return;
      }
      
      this.server.close((err) => {
        if (err) {
          reject(err);
        } else {
          this.server = null;
          console.log('Health server stopped');
          resolve();
        }
      });
    });
  }
  
  /**
   * Handle /health endpoint
   */
  private handleHealthCheck(req: http.IncomingMessage, res: http.ServerResponse): void {
    const metricsData = metrics.toJSON();
    const response = {
      status: 'healthy',
      metrics: metricsData
    };
    
    res.setHeader('Content-Type', 'application/json');
    res.statusCode = 200;
    res.end(JSON.stringify(response));
  }
  
  /**
   * Handle /metrics endpoint
   */
  private handleMetrics(req: http.IncomingMessage, res: http.ServerResponse): void {
    const metricsData = metrics.toJSON();
    
    res.setHeader('Content-Type', 'application/json');
    res.statusCode = 200;
    res.end(JSON.stringify(metricsData));
  }
}

// Create singleton instance
let healthServerInstance: HealthServer | null = null;

/**
 * Initialize the health server
 * @param port Port to listen on
 */
export function initHealthServer(port: number = parseInt(process.env.HEALTH_PORT || '8093')): HealthServer {
  if (!healthServerInstance) {
    healthServerInstance = new HealthServer(port);
  }
  return healthServerInstance;
}

/**
 * Get the health server instance
 */
export function getHealthServer(): HealthServer | null {
  return healthServerInstance;
}