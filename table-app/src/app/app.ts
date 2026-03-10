import { ChangeDetectionStrategy, Component, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';

type UserRow = {
  id: number;
  name: string;
  role: string;
  email: string;
};

@Component({
  selector: 'app-root',
  imports: [],
  templateUrl: './app.html',
  styleUrl: './app.scss',
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class App {
  constructor(private readonly http: HttpClient) {}

  protected readonly title = 'Simple Users Table';
  protected readonly helloMessage = signal('');
  protected readonly errorMessage = signal('');

  protected readonly users: UserRow[] = [
    { id: 1, name: 'Alice Johnson', role: 'Admin', email: 'alice@example.com' },
    { id: 2, name: 'David Smith', role: 'Editor', email: 'david@example.com' },
    { id: 3, name: 'Maria Garcia', role: 'Viewer', email: 'maria@example.com' },
    { id: 4, name: 'Liam Brown', role: 'Editor', email: 'liam@example.com' }
  ];

  protected getHelloMessage(): void {
    this.errorMessage.set('');
    this.http.get<{ message: string }>('/api/hello').subscribe({
      next: (response) => {
        this.helloMessage.set(response.message);
      },
      error: () => {
        this.helloMessage.set('');
        this.errorMessage.set('Could not load message from server.');
      }
    });
  }
}
