import { ChangeDetectionStrategy, Component, OnDestroy, OnInit, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';

type UserRow = {
  id: number;
  name: string;
  role: string;
  email: string;
};

type DatabricksCurrentUserResponse = {
  user: {
    current_user?: string;
    current_catalog?: string;
    current_schema?: string;
  } | null;
};

type PersonsResponse = {
  rows: UserRow[];
  rowCount: number;
};

type JobRunStatus = {
  state: {
    life_cycle_state: string;
    result_state?: string;
    state_message?: string;
  };
};

const TERMINAL_STATES = ['TERMINATED', 'SKIPPED', 'INTERNAL_ERROR'];

@Component({
  selector: 'app-root',
  imports: [],
  templateUrl: './app.html',
  styleUrl: './app.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class App implements OnInit, OnDestroy {
  constructor(private readonly http: HttpClient) {}

  protected readonly title = 'Simple Users Table';
  protected readonly helloMessage = signal('');
  protected readonly errorMessage = signal('');
  protected readonly users = signal<UserRow[]>([]);
  protected readonly loading = signal(false);
  protected readonly jobRunning = signal(false);
  protected readonly jobMessage = signal('');
  protected readonly jobStatus = signal('');

  private readonly JOB_ID = 538667917154866;
  private pollInterval: ReturnType<typeof setInterval> | null = null;

  ngOnInit(): void {
    this.loadPersons();
  }

  ngOnDestroy(): void {
    this.stopPolling();
  }

  private stopPolling(): void {
    if (this.pollInterval !== null) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
    }
  }

  private pollJobStatus(runId: number): void {
    this.pollInterval = setInterval(() => {
      this.http.get<JobRunStatus>(`/api/databricks/jobs/run/${runId}/status`).subscribe({
        next: (response) => {
          const { life_cycle_state, result_state } = response.state;
          this.jobStatus.set(`Status: ${life_cycle_state}${result_state ? ' — ' + result_state : ''}`);

          if (TERMINAL_STATES.includes(life_cycle_state)) {
            this.stopPolling();
            this.jobRunning.set(false);
          }
        },
        error: () => {
          this.stopPolling();
          this.jobRunning.set(false);
        },
      });
    }, 5000);
  }

  private loadPersons(): void {
    this.loading.set(true);
    this.errorMessage.set('');
    this.http.get<PersonsResponse>('/api/databricks/unity-catalog/persons').subscribe({
      next: (response) => {
        this.users.set(response.rows);
        this.loading.set(false);
      },
      error: (error: any) => {
        console.log('Error loading persons from Databricks:', error);
        this.errorMessage.set('Could not load persons from Databricks.');
        this.loading.set(false);
      },
    });
  }

  protected runJob(): void {
    this.jobMessage.set('');
    this.jobStatus.set('');
    this.errorMessage.set('');
    this.jobRunning.set(true);
    this.http.post<{ run_id: number }>('/api/databricks/jobs/run', { job_id: this.JOB_ID }).subscribe({
      next: (response) => {
        this.jobMessage.set(`Job started. Run ID: ${response.run_id}`);
        this.pollJobStatus(response.run_id);
      },
      error: () => {
        this.errorMessage.set('Failed to trigger the Databricks job.');
        this.jobRunning.set(false);
      },
    });
  }

  protected getHelloMessage(): void {
    this.errorMessage.set('');
    this.http.get<{ message: string }>('/api/hello').subscribe({
      next: (response) => {
        this.helloMessage.set(response.message);
        this.http.get<DatabricksCurrentUserResponse>('/api/databricks/current-user').subscribe({
          next: (databricksResponse) => {
            console.log('Databricks current user details:', databricksResponse.user);
          },
          error: (error) => {
            console.error('Failed to fetch Databricks current user details:', error);
          },
        });
      },
      error: () => {
        this.helloMessage.set('');
        this.errorMessage.set('Could not load message from server.');
      },
    });
  }
}
