import NextHead from 'next/head';
import { PRODUCT_NAME, buildBrandMetadata } from '@components/Brand/brand';
import { useContext } from 'react';
import { useRouter } from 'next/router';
import { ThemeContext } from 'styled-components';

type HeadProps = {
  children?: any;
  defaultTitle?: string;
  title?: string;
};

const Head = ({ children, defaultTitle = PRODUCT_NAME, title }: HeadProps) => {
  const theme = useContext(ThemeContext);
  const router = useRouter();
  const metadata = buildBrandMetadata({ assetBase: router?.basePath || '' });

  return (
    <NextHead>
      <title>{title ? `${title} | ${defaultTitle}` : defaultTitle}</title>

      <link href={metadata.fallbackIcon.href} key="mageml-favicon-fallback" rel="icon" sizes="any" />
      <link href={metadata.icon.href} key="mageml-favicon" rel="icon" type="image/svg+xml" />

      <style>{`
        html body {
          background-color: ${theme?.backgrounds?.body};
        }
      `}</style>

      {children}
    </NextHead>
  );
};

export default Head;
