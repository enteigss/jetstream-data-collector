import cron from 'node-cron'
import { Pool } from 'pg'
import { Server } from 'socket.io'
import { AggregationService } from './aggregation-service'

export class MetricsScheduler {
    private pool: Pool;
    private io: Server;
    private aggregationService: AggregationService;
    private tasks: Map<string, cron.ScheduledTask> = new Map();
    private isRunning: boolean = false;

    constructor(pool: Pool, io: Server) {
        this.pool = pool;
        this.io = io;
        this.aggregationService = new AggregationService(pool, io);
    }

    async initialize() {
        try {
          // Initialize metrics schema and tables
          await this.aggregationService.resetDb();
          console.log('Metrics system initialized successfully');
          return true;
        } catch (error) {
          console.error('Failed to initialize metrics system:', error);
          throw error;
        }
      }

    async start() {
        if (this.isRunning) {
            console.log('Scheduler is already running')
            return
        }

        try {
            this.tasks.set('shortTerm', cron.schedule('*/10 * * * *', async () => {
                try {
                    await this.aggregationService.computeShortTermMetrics()
                } catch (error) {
                    console.error('Error in short-term metrics jobs:', error)
                }
            }))

            await this.runInitialComputations()

            this.isRunning = true
            console.log('Metrics scheduler started successfully')
            return true
        } catch (error) {
            console.error('Error starting metrics scheduler:', error)
            this.stop()
            throw error
        }
    }

    stop() {
        for (const [name, task] of this.tasks.entries()) {
          task.stop();
          console.log(`Stopped ${name} task`);
        }
        this.tasks.clear();
        this.isRunning = false;
        console.log('Metrics scheduler stopped');
    }

    private async runInitialComputations() {
        try {
          console.log('Running initial metric computations...');
          
          // Run in parallel for efficiency
          await Promise.all([
            this.aggregationService.computeShortTermMetrics()
          ]);
          
          console.log('Initial metric computations completed');
        } catch (error) {
          console.error('Error in initial metric computations:', error);
        }
      }
    
      
}