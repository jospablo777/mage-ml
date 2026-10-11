import React from 'react';
import { UNIT } from '@oracle/styles/units/spacing';

interface ImageOutputProps {
  data: string;
  height?: number;
  uuid?: string;
}

function ImageOutput({ data, height = UNIT * 60, uuid }: ImageOutputProps): JSX.Element {
  return (
    <div
      style={{
        backgroundColor: '#000000',
        height,
        overflow: 'auto',
        position: 'relative',
      }}
    >
      {/* A plain img: next/image only adds optimization, which the static export turns off. */}
      {/* eslint-disable-next-line @next/next/no-img-element */}
      <img
        alt={`Image ${uuid ? `${uuid} ` : ''}from code output`}
        src={`data:image/png;base64, ${data}`}
        style={{ height: '100%', objectFit: 'contain', width: '100%' }}
      />
    </div>
  );
}

export default ImageOutput;
