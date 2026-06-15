import { render, screen } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';
import IncidentTable from './IncidentTable.svelte';
import type { LocalIncident } from '../lib/types';

function incident(overrides: Partial<LocalIncident> = {}): LocalIncident {
  return {
    id: 'incident-1',
    at: 1_700_013_600_000,
    scope: '豆粕 2609',
    scope_symbol: 'DCE.m2609',
    type: '状态切换',
    detail: '从实时监控中切换为延迟预警',
    impact: '需关注',
    severity: 'warn',
    ...overrides,
  };
}

describe('IncidentTable', () => {
  it('uses shared panel, count, and severity badge utilities', () => {
    const incidents = [incident()];
    const { container } = render(IncidentTable, { incidents });

    const panel = screen.getByTestId('incident-table');
    expect(panel.className).toContain('panel-shell');
    expect(panel.className).toContain('flex');
    expect(panel.className).toContain('min-h-0');
    expect(panel.className).toContain('flex-col');
    expect(panel.className).toContain('px-3');
    expect(panel.className).toContain('py-2.5');
    expect(panel.className).not.toContain('incidents');

    const count = screen.getByText('1');
    expect(count.className).toContain('count-chip');
    expect(count.className).toContain('bg-[#42a7ff22]');

    const badge = screen.getByText('状态切换');
    expect(badge.className).toContain('severity-badge');
    expect(` ${badge.className} `).not.toContain(' badge ');

    const list = container.querySelector('.relative.z-\\[1\\]');
    expect(list?.className).toContain('overflow-y-auto');
    expect(list?.className).toContain('content-start');

    const item = badge.closest('div[class*="rounded"]');
    expect(item?.className).toContain('rounded-[6px]');
    expect(item?.className).toContain('border-[#ffc44744]');
  });
});
