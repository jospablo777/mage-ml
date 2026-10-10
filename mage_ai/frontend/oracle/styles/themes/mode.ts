// The color theme the user chose: dark (the default) or light, kept in a cookie so every
// page, including the first one a static export serves, can read it before rendering.
export const THEME_COOKIE: 'current_theme' = 'current_theme';
export const THEME_DARK = 0;
export const THEME_LIGHT = 1;

export enum ThemeModeEnum {
  DARK = 'dark',
  LIGHT = 'light',
}

export function themeModeFromCookie(value?: string | number | null): ThemeModeEnum {
  return Number(value) === THEME_LIGHT ? ThemeModeEnum.LIGHT : ThemeModeEnum.DARK;
}

function readCookie(name: string): string | null {
  if (typeof document === 'undefined') {
    return null;
  }
  const match = document.cookie.match(new RegExp(`(?:^|; )${name}=([^;]*)`));
  return match ? decodeURIComponent(match[1]) : null;
}

// The mode in this browser; dark on the server and at build time.
export function browserThemeMode(): ThemeModeEnum {
  try {
    return themeModeFromCookie(readCookie(THEME_COOKIE));
  } catch {
    return ThemeModeEnum.DARK;
  }
}

export function isLightTheme(): boolean {
  return browserThemeMode() === ThemeModeEnum.LIGHT;
}

// Runs in <head> before the page renders: a light page stays hidden until React has drawn
// it in the light theme, so the prebuilt dark HTML never flashes.
export const THEME_BOOT_SCRIPT = `
(function() {
  try {
    if (!/(?:^|; )${THEME_COOKIE}=${THEME_LIGHT}(?:;|$)/.test(document.cookie)) return;
    var root = document.documentElement;
    root.setAttribute('data-theme', 'light');
    var style = document.createElement('style');
    style.textContent = 'html[data-theme=light]{background:#FFFFFF;color-scheme:light}' +
      'html[data-theme=light]:not([data-theme-ready]) body{visibility:hidden}';
    document.head.appendChild(style);
    setTimeout(function() { root.setAttribute('data-theme-ready', ''); }, 4000);
  } catch (e) {}
})();
`;
