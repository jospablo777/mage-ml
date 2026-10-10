import darkPalette from './darkPalette';
import lightPalette from './light';
import { isLightTheme } from './mode';

// The palette components use when they read colors without the styled-components theme:
// the dark palette unless the user chose the light theme in this browser. The name stays
// "dark" because the components import it under that name; darkPalette.ts holds the dark
// colors themselves.
const activePalette = (isLightTheme() ? lightPalette : darkPalette) as typeof darkPalette;

export default activePalette;
