import { isLightTheme } from '@oracle/styles/themes/mode';

export const DEFAULT_AUTO_SAVE_INTERVAL = 5000;
export const DEFAULT_LANGUAGE = 'python';

// Editors follow the app's theme.
export const DEFAULT_THEME = isLightTheme() ? 'github' : 'twilight';
