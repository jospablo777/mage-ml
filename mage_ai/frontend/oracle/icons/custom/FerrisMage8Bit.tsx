import styled from 'styled-components';

import { BaseIconProps } from '../BaseIcon';

const SVGStyle = styled.svg``;

// Ferris, the Rust crab, as a mage: a 16 by 16 pixel drawing, one path per color.
//   ..........y.....
//   ...........H....
//   ..........HH..s.
//   .........HhH..|.
//   ........HHhH..|.
//   .......HHYHHH.|.
//   ......HHHHHHH.|.
//   ....HHHHHHHHHHH.
//   ...W.K......KWC|
//   C...k........kCC
//   CC..k........k.|
//   CCCCCCCCCCCCCCC|
//   .CCCoCCCCCCoCCC.
//   ..CCCCCmmCCCCC..
//   ..c.c.cccc.c.c..
//   .c.c........c.c.
const FerrisMage8Bit = ({
  size = 20,
  viewBox = '0 0 16 16',
}: BaseIconProps) => (
  <SVGStyle
    height={size}
    shapeRendering="crispEdges"
    viewBox={viewBox}
    width={size}
  >
    {/* hat */}
    <path d="M11 1h1v1h-1zM10 2h1v1h-1zM11 2h1v1h-1zM9 3h1v1h-1zM11 3h1v1h-1zM8 4h1v1h-1zM9 4h1v1h-1zM11 4h1v1h-1zM7 5h1v1h-1zM8 5h1v1h-1zM10 5h1v1h-1zM11 5h1v1h-1zM12 5h1v1h-1zM6 6h1v1h-1zM7 6h1v1h-1zM8 6h1v1h-1zM9 6h1v1h-1zM10 6h1v1h-1zM11 6h1v1h-1zM12 6h1v1h-1zM4 7h1v1h-1zM5 7h1v1h-1zM6 7h1v1h-1zM7 7h1v1h-1zM8 7h1v1h-1zM9 7h1v1h-1zM10 7h1v1h-1zM11 7h1v1h-1zM12 7h1v1h-1zM13 7h1v1h-1zM14 7h1v1h-1z" fill="#7949FF" />
    {/* hat light */}
    <path d="M10 3h1v1h-1zM10 4h1v1h-1z" fill="#AF93FF" />
    {/* hat star */}
    <path d="M9 5h1v1h-1z" fill="#FFCC19" />
    {/* hat tip spark */}
    <path d="M10 0h1v1h-1z" fill="#2ECDF7" />
    {/* staff spark */}
    <path d="M14 2h1v1h-1z" fill="#2ECDF7" />
    {/* staff */}
    <path d="M14 3h1v1h-1zM14 4h1v1h-1zM14 5h1v1h-1zM14 6h1v1h-1zM15 8h1v1h-1zM15 10h1v1h-1zM15 11h1v1h-1z" fill="#FFCC19" />
    {/* shell */}
    <path d="M14 8h1v1h-1zM0 9h1v1h-1zM14 9h1v1h-1zM15 9h1v1h-1zM0 10h1v1h-1zM1 10h1v1h-1zM0 11h1v1h-1zM1 11h1v1h-1zM2 11h1v1h-1zM3 11h1v1h-1zM4 11h1v1h-1zM5 11h1v1h-1zM6 11h1v1h-1zM7 11h1v1h-1zM8 11h1v1h-1zM9 11h1v1h-1zM10 11h1v1h-1zM11 11h1v1h-1zM12 11h1v1h-1zM13 11h1v1h-1zM14 11h1v1h-1zM1 12h1v1h-1zM2 12h1v1h-1zM3 12h1v1h-1zM5 12h1v1h-1zM6 12h1v1h-1zM7 12h1v1h-1zM8 12h1v1h-1zM9 12h1v1h-1zM10 12h1v1h-1zM12 12h1v1h-1zM13 12h1v1h-1zM14 12h1v1h-1zM2 13h1v1h-1zM3 13h1v1h-1zM4 13h1v1h-1zM5 13h1v1h-1zM6 13h1v1h-1zM9 13h1v1h-1zM10 13h1v1h-1zM11 13h1v1h-1zM12 13h1v1h-1zM13 13h1v1h-1z" fill="#F74C00" />
    {/* legs */}
    <path d="M2 14h1v1h-1zM4 14h1v1h-1zM6 14h1v1h-1zM7 14h1v1h-1zM8 14h1v1h-1zM9 14h1v1h-1zM11 14h1v1h-1zM13 14h1v1h-1zM1 15h1v1h-1zM3 15h1v1h-1zM12 15h1v1h-1zM14 15h1v1h-1z" fill="#B83A00" />
    {/* eye stalks */}
    <path d="M4 9h1v1h-1zM13 9h1v1h-1zM4 10h1v1h-1zM13 10h1v1h-1z" fill="#B83A00" />
    {/* cheeks */}
    <path d="M4 12h1v1h-1zM11 12h1v1h-1z" fill="#FF9466" />
    {/* eyes */}
    <path d="M3 8h1v1h-1zM13 8h1v1h-1z" fill="#FFFFFF" />
    {/* pupils */}
    <path d="M5 8h1v1h-1zM12 8h1v1h-1z" fill="#1B1B1F" />
    {/* mouth */}
    <path d="M7 13h1v1h-1zM8 13h1v1h-1z" fill="#1B1B1F" />
  </SVGStyle>
);

export default FerrisMage8Bit;
