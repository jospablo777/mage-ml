import styled from 'styled-components';

import {
  FONT_FAMILY_MEDIUM,
  FONT_FAMILY_REGULAR,
  MONO_FONT_FAMILY_MEDIUM,
  MONO_FONT_FAMILY_REGULAR,
} from '@oracle/styles/fonts/primary';
import { HEADER_HEIGHT, ROW_HEIGHT } from './logic';

// Mage's theme as CSS variables, so the explorer follows the dark and light themes.
export function themeVariables(theme: any): Record<string, string> {
  return {
    '--ca-accent': theme?.elevation?.visualizationAccent || '#996CFF',
    '--ca-accent-alt': theme?.elevation?.visualizationAccentAlt || '#C1ACF7',
    '--ca-bg': theme?.background?.table || '#292A2F',
    '--ca-border': theme?.borders?.light || '#2F3034',
    '--ca-border-strong': theme?.borders?.button || '#454850',
    '--ca-elevated': theme?.background?.popup || '#27292E',
    '--ca-muted': theme?.content?.muted || '#9DA1AF',
    '--ca-negative': theme?.accent?.negative || '#FF1E59',
    '--ca-positive': theme?.accent?.positive || '#00A81A',
    '--ca-share-1': theme?.accent?.cyan || '#65E3FF',
    '--ca-share-2': theme?.accent?.yellow || '#FFCC19',
    '--ca-share-3': theme?.accent?.rose || '#D1A2AB',
    '--ca-surface': theme?.background?.panel || '#232429',
    '--ca-text': theme?.content?.default || '#FFFFFF',
    '--ca-warning': theme?.accent?.warning || '#DD9900',
  };
}

// Mage loads one font family per weight, so bold text uses the medium family, not font-weight.
const MONO = MONO_FONT_FAMILY_REGULAR;
const MONO_MEDIUM = MONO_FONT_FAMILY_MEDIUM;
const MEDIUM = FONT_FAMILY_MEDIUM;

export const AtlasRoot = styled.section<{ $mode: 'inline' | 'expanded' }>`
  background: var(--ca-bg);
  border: 1px solid var(--ca-border);
  border-radius: ${props => (props.$mode === 'expanded' ? '0' : '8px')};
  box-sizing: border-box;
  color: var(--ca-text);
  display: flex;
  flex-direction: column;
  font-family: ${FONT_FAMILY_REGULAR};
  font-size: 12px;
  height: ${props => (props.$mode === 'expanded' ? '100%' : 'auto')};
  min-width: 0;
  overflow: hidden;
  position: relative;

  *, *::before, *::after { box-sizing: border-box; }
  button, input, select { color: inherit; font: inherit; }
  button { cursor: pointer; }
  strong { font-family: ${MEDIUM}; font-weight: normal; }
  :focus-visible { outline: 2px solid var(--ca-accent); outline-offset: -2px; }

  .ca-toolbar {
    align-items: center;
    background: var(--ca-surface);
    border-bottom: 1px solid var(--ca-border);
    display: flex;
    gap: 12px;
    justify-content: space-between;
    min-height: 42px;
    padding: 6px 10px 6px 14px;
  }
  .ca-title { display: flex; align-items: baseline; gap: 10px; min-width: 0; }
  .ca-title strong {
    font-family: ${MONO_MEDIUM};
    font-size: 12px;
   
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .ca-title span { color: var(--ca-muted); font-variant-numeric: tabular-nums; white-space: nowrap; }
  .ca-actions { display: flex; gap: 6px; flex: none; }
  .ca-button {
    background: transparent;
    border: 1px solid var(--ca-border-strong);
    border-radius: 6px;
    line-height: 16px;
    padding: 4px 10px;
    white-space: nowrap;
  }
  .ca-button:hover { border-color: var(--ca-accent); }
  .ca-button-active {
    background: color-mix(in srgb, var(--ca-accent) 18%, transparent);
    border-color: var(--ca-accent);
  }
  .ca-button-primary {
    background: var(--ca-accent);
    border-color: var(--ca-accent);
    color: #fff;
    font-family: ${MEDIUM};
  }

  .ca-filterbar {
    align-items: center;
    background: var(--ca-surface);
    border-bottom: 1px solid var(--ca-border);
    display: flex;
    flex-wrap: wrap;
    gap: 6px 8px;
    padding: 7px 12px;
  }
  .ca-chip {
    align-items: center;
    background: color-mix(in srgb, var(--ca-accent) 16%, transparent);
    border: 1px solid color-mix(in srgb, var(--ca-accent) 45%, transparent);
    border-radius: 12px;
    display: inline-flex;
    font-family: ${MONO};
    font-size: 11px;
    gap: 4px;
    padding: 2px 4px 2px 10px;
    max-width: 100%;
  }
  .ca-chip button { background: none; border: 0; border-radius: 50%; color: var(--ca-muted); padding: 0 5px; }
  .ca-chip button:hover { color: var(--ca-text); }
  .ca-sort-summary { color: var(--ca-muted); }
  .ca-filter-editor {
    align-items: center;
    display: flex;
    flex-basis: 100%;
    flex-wrap: wrap;
    gap: 6px 8px;
  }
  .ca-filter-editor label { color: var(--ca-muted); font-size: 11px; }
  .ca-filter-editor select, .ca-filter-editor input, .ca-panel-head input, .ca-jump input {
    background: var(--ca-bg);
    border: 1px solid var(--ca-border-strong);
    border-radius: 6px;
    min-height: 28px;
    padding: 3px 8px;
  }
  .ca-filter-editor select { max-width: 220px; }
  .ca-filter-editor input { width: 170px; }
  .ca-filter-actions { align-items: center; display: flex; gap: 6px; margin-left: auto; }

  .ca-body { display: flex; flex: 1; min-height: 0; position: relative; }
  .ca-grid-area { display: flex; flex: 1; flex-direction: column; min-width: 0; position: relative; }
  .ca-scroll {
    flex: 1;
    min-height: 0;
    overflow: auto;
    overscroll-behavior: contain;
    position: relative;
    scrollbar-width: thin;
  }
  .ca-scroll:focus-visible { outline-offset: -2px; }
  /* clip, not hidden: rows past the end of a scaled table must not grow the scroll area,
     and the sticky header still needs the scroller as its scroll container. */
  .ca-stage {
    overflow: clip;
    position: relative;
  }

  .ca-header {
    background: var(--ca-surface);
    border-bottom: 1px solid var(--ca-border-strong);
    position: sticky;
    top: 0;
    z-index: 4;
  }
  .ca-corner {
    align-items: center;
    background: var(--ca-surface);
    border-right: 1px solid var(--ca-border);
    color: var(--ca-muted);
    display: flex;
    height: ${HEADER_HEIGHT}px;
    justify-content: flex-end;
    left: 0;
    padding-right: 10px;
    position: sticky;
    z-index: 5;
  }
  .ca-head {
    border-right: 1px solid var(--ca-border);
    display: flex;
    flex-direction: column;
    height: ${HEADER_HEIGHT}px;
    position: absolute;
    top: 0;
  }
  .ca-head:hover .ca-head-filter { opacity: 1; }
  .ca-head-sorted { background: color-mix(in srgb, var(--ca-accent) 9%, transparent); }
  .ca-head-main {
    background: none;
    border: 0;
    display: flex;
    flex-direction: column;
    gap: 1px;
    min-width: 0;
    padding: 6px 26px 0 10px;
    text-align: left;
  }
  .ca-head-name { align-items: center; display: flex; gap: 6px; min-width: 0; }
  .ca-head-text { font-family: ${MEDIUM}; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .ca-head-type {
    color: var(--ca-muted);
    font-family: ${MONO};
    font-size: 10px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .ca-kind { color: var(--ca-accent); flex: none; font-family: ${MONO}; font-size: 10px; min-width: 14px; }
  .ca-sort-mark { color: var(--ca-accent); flex: none; font-family: ${MEDIUM}; }
  .ca-head-profile { cursor: pointer; padding: 2px 10px 0; }
  .ca-head-filter {
    background: none;
    border: 0;
    color: var(--ca-muted);
    font-size: 13px;
    opacity: 0;
    padding: 2px 6px;
    position: absolute;
    right: 4px;
    top: 4px;
  }
  .ca-head-filter:focus-visible { opacity: 1; }
  .ca-head-filter:hover { color: var(--ca-text); }
  .ca-resize { bottom: 0; cursor: col-resize; position: absolute; right: -3px; top: 0; width: 6px; z-index: 2; }
  .ca-resize:hover { background: var(--ca-accent); }

  .ca-profile { align-items: flex-end; display: flex; gap: 6px; height: 18px; }
  .ca-profile-wait::after {
    animation: ca-pulse 1.2s ease-in-out infinite;
    background: var(--ca-border);
    border-radius: 2px;
    content: '';
    height: 6px;
    width: 64px;
  }
  .ca-spark rect { fill: var(--ca-accent); }
  .ca-spark rect.ca-spark-hover, .ca-histogram rect.ca-spark-hover { fill: var(--ca-accent-alt); }
  .ca-spark rect.ca-hit, .ca-histogram rect.ca-hit { cursor: default; fill: transparent; }
  .ca-spark-shares rect.ca-spark-hover { opacity: 0.75; }
  .ca-tip {
    background: var(--ca-elevated);
    border: 1px solid var(--ca-border-strong);
    border-radius: 4px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.35);
    color: var(--ca-text);
    font-family: ${MONO};
    font-size: 11px;
    line-height: 16px;
    overflow-wrap: anywhere;
    padding: 4px 8px;
    pointer-events: none;
    position: absolute;
    white-space: pre-line;
    z-index: 30;
  }
  .ca-spark .ca-spark-track { fill: var(--ca-border); }
  .ca-share-0, .ca-bar .ca-share-0 { fill: var(--ca-accent); background: var(--ca-accent); }
  .ca-share-1, .ca-bar .ca-share-1 { fill: var(--ca-share-1); background: var(--ca-share-1); }
  .ca-share-2, .ca-bar .ca-share-2 { fill: var(--ca-share-2); background: var(--ca-share-2); }
  .ca-share-3, .ca-bar .ca-share-3 { fill: var(--ca-share-3); background: var(--ca-share-3); }
  .ca-distinct-mark { color: var(--ca-muted); font-size: 10px; white-space: nowrap; }
  .ca-missing-mark { color: var(--ca-warning); font-size: 10px; white-space: nowrap; }

  .ca-row { height: ${ROW_HEIGHT}px; left: 0; position: absolute; }
  .ca-row-odd { background: color-mix(in srgb, var(--ca-surface) 45%, transparent); }
  .ca-row:hover { background: color-mix(in srgb, var(--ca-accent) 8%, transparent); }
  .ca-index {
    align-items: center;
    background: var(--ca-surface);
    border-right: 1px solid var(--ca-border);
    color: var(--ca-muted);
    display: flex;
    font-size: 11px;
    font-variant-numeric: tabular-nums;
    height: ${ROW_HEIGHT}px;
    justify-content: flex-end;
    left: 0;
    padding-right: 10px;
    position: sticky;
    z-index: 2;
  }
  .ca-cell {
    border-bottom: 1px solid color-mix(in srgb, var(--ca-border) 70%, transparent);
    border-right: 1px solid color-mix(in srgb, var(--ca-border) 70%, transparent);
    cursor: default;
    font-family: ${MONO};
    font-size: 12px;
    height: ${ROW_HEIGHT}px;
    line-height: ${ROW_HEIGHT - 1}px;
    overflow: hidden;
    padding: 0 10px;
    position: absolute;
    text-overflow: ellipsis;
    top: 0;
    user-select: text;
    white-space: nowrap;
  }
  .ca-cell-number { font-variant-numeric: tabular-nums; text-align: right; }
  .ca-cell-missing { color: var(--ca-muted); font-family: inherit; font-style: italic; }
  .ca-cell-loading::after {
    animation: ca-pulse 1.2s ease-in-out infinite;
    background: var(--ca-border);
    border-radius: 3px;
    content: '';
    display: inline-block;
    height: 8px;
    vertical-align: middle;
    width: 60%;
  }
  .ca-cell-selected {
    box-shadow: inset 0 0 0 2px var(--ca-accent);
    background: color-mix(in srgb, var(--ca-accent) 14%, transparent);
  }

  .ca-empty {
    color: var(--ca-muted);
    left: 0;
    padding: 24px;
    position: sticky;
    text-align: center;
    top: ${HEADER_HEIGHT}px;
  }
  .ca-banner {
    background: color-mix(in srgb, var(--ca-negative) 18%, var(--ca-surface));
    border-top: 1px solid var(--ca-negative);
    padding: 8px 12px;
  }
  .ca-inspector {
    background: var(--ca-elevated);
    border-top: 1px solid var(--ca-border-strong);
    box-shadow: 0 -8px 24px rgba(0, 0, 0, 0.35);
    max-height: 45%;
    overflow: auto;
    padding: 8px 12px 12px;
  }
  .ca-inspector-head { align-items: center; display: flex; gap: 8px; justify-content: space-between; }
  .ca-inspector-head > span:last-child { display: flex; gap: 6px; }
  .ca-inspector pre { font-family: ${MONO}; margin: 8px 0 0; white-space: pre-wrap; word-break: break-word; }

  .ca-status {
    align-items: center;
    background: var(--ca-surface);
    border-top: 1px solid var(--ca-border);
    color: var(--ca-muted);
    display: flex;
    font-size: 11px;
    font-variant-numeric: tabular-nums;
    gap: 14px;
    min-height: 32px;
    padding: 3px 10px 3px 14px;
  }
  .ca-hint { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .ca-jump { margin-left: auto; }
  .ca-jump label { align-items: center; display: flex; gap: 6px; }
  .ca-jump input { min-height: 24px; width: 92px; }
  .ca-spinner {
    animation: ca-spin 0.8s linear infinite;
    border: 2px solid var(--ca-border-strong);
    border-radius: 50%;
    border-top-color: var(--ca-accent);
    height: 12px;
    width: 12px;
  }

  .ca-panel {
    background: var(--ca-surface);
    border-left: 1px solid var(--ca-border);
    display: flex;
    flex: none;
    flex-direction: column;
    min-height: 0;
    overflow: hidden;
    width: 300px;
  }
  .ca-panel-head { display: flex; gap: 6px; padding: 10px; }
  .ca-panel-head input { flex: 1; min-width: 0; }
  .ca-column-list {
    flex: 0 1 auto;
    list-style: none;
    margin: 0;
    max-height: 38%;
    overflow: auto;
    padding: 0 0 6px;
    scrollbar-width: thin;
  }
  .ca-column-list button {
    align-items: center;
    background: none;
    border: 0;
    border-left: 2px solid transparent;
    display: flex;
    gap: 8px;
    padding: 5px 12px;
    text-align: left;
    width: 100%;
  }
  .ca-column-list button:hover { background: color-mix(in srgb, var(--ca-accent) 9%, transparent); }
  .ca-column-list .ca-column-active {
    background: color-mix(in srgb, var(--ca-accent) 16%, transparent);
    border-left-color: var(--ca-accent);
  }
  .ca-column-name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .ca-column-list code {
    color: var(--ca-muted);
    font-size: 10px;
    max-width: 96px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .ca-list-note { padding: 6px 12px; }

  .ca-detail, .ca-detail-wait {
    border-top: 1px solid var(--ca-border);
    flex: 1;
    min-height: 0;
    overflow: auto;
    padding: 12px 14px 16px;
    scrollbar-width: thin;
  }
  .ca-detail-wait { color: var(--ca-muted); }
  .ca-detail-head { align-items: baseline; display: flex; gap: 8px; justify-content: space-between; }
  .ca-detail-head strong { font-size: 13px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .ca-detail-head code { color: var(--ca-muted); flex: none; font-size: 10px; }
  .ca-detail h4 {
    color: var(--ca-muted);
    font-size: 11px;
    font-family: ${MEDIUM};
    margin: 14px 0 6px;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .ca-completeness {
    background: var(--ca-warning);
    border-radius: 3px;
    height: 4px;
    margin: 10px 0 4px;
    overflow: hidden;
  }
  .ca-completeness span { background: var(--ca-positive); display: block; height: 100%; }
  .ca-metrics { display: grid; gap: 0 12px; grid-template-columns: 1fr 1fr; margin: 8px 0 0; }
  .ca-metric {
    border-bottom: 1px solid color-mix(in srgb, var(--ca-border) 70%, transparent);
    display: flex;
    justify-content: space-between;
    gap: 6px;
    padding: 5px 0;
    min-width: 0;
  }
  .ca-metric dt { color: var(--ca-muted); white-space: nowrap; }
  .ca-metric dd {
    font-family: ${MONO};
    font-variant-numeric: tabular-nums;
    margin: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .ca-histogram { margin: 12px 0 4px; }
  .ca-histogram svg { display: block; height: auto; width: 100%; }
  .ca-histogram rect { fill: var(--ca-accent); }
  .ca-histogram figcaption {
    color: var(--ca-muted);
    display: flex;
    font-family: ${MONO};
    font-size: 10px;
    justify-content: space-between;
    margin-top: 3px;
  }
  .ca-top-values { display: grid; gap: 8px; list-style: none; margin: 0; padding: 0; }
  .ca-top-value-head, .ca-top-other { display: flex; gap: 8px; justify-content: space-between; }
  .ca-top-value-head span:first-child { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .ca-top-other { color: var(--ca-muted); }
  .ca-numeric-text { flex: none; font-variant-numeric: tabular-nums; }
  .ca-numeric-text small { color: var(--ca-muted); margin-left: 6px; }
  .ca-bar { background: var(--ca-border); border-radius: 3px; height: 4px; margin-top: 3px; overflow: hidden; }
  .ca-bar span { display: block; height: 100%; }
  .ca-muted-text { color: var(--ca-muted); }
  .ca-error-text { color: var(--ca-negative); }

  &.ca-narrow .ca-panel {
    bottom: 0;
    box-shadow: -10px 0 28px rgba(0, 0, 0, 0.45);
    position: absolute;
    right: 0;
    top: 0;
    width: min(320px, 92%);
    z-index: 8;
  }
  &.ca-narrow .ca-hint, &.ca-narrow .ca-sort-summary { display: none; }
  &.ca-narrow .ca-toolbar { flex-wrap: wrap; }

  @keyframes ca-pulse { 50% { opacity: 0.4; } }
  @keyframes ca-spin { to { transform: rotate(360deg); } }
  @media (prefers-reduced-motion: reduce) {
    .ca-profile-wait::after, .ca-cell-loading::after, .ca-spinner { animation: none; }
  }
`;
