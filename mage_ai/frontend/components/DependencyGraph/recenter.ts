import { CanvasRef } from 'reaflow';

// Value of reaflow's CanvasPosition.CENTER. Used as a literal so this module stays
// free of a runtime import from reaflow, which is loaded dynamically.
const CANVAS_POSITION_CENTER = 'center';

/**
 * Fit the graph to the viewport and center it.
 *
 * fitCanvas sets the zoom and positions the canvas in the same tick, so reaflow
 * computes the offset from the zoom it had before the fit. Centering again after
 * React commits the new zoom puts the graph where the fit intended.
 */
export function recenterCanvas(canvasRef?: { current?: CanvasRef }): void {
  const canvas = canvasRef?.current;

  if (!canvas?.fitCanvas) {
    return;
  }

  canvas.fitCanvas();

  if (typeof requestAnimationFrame === 'undefined') {
    return;
  }

  requestAnimationFrame(() => {
    const current = canvasRef?.current;

    if (current?.positionCanvas) {
      // @ts-ignore reaflow types this argument with its own enum
      current.positionCanvas(CANVAS_POSITION_CENTER);
    } else {
      current?.fitCanvas?.();
    }
  });
}
