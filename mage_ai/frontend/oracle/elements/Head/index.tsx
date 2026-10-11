import NextHead from 'next/head';
import { PRODUCT_NAME, buildBrandMetadata } from '@components/Brand/brand';
import { useRouter } from 'next/router';

type HeadProps = {
  children?: any;
  defaultTitle?: string;
  title?: string;
};

const Head = ({ children, defaultTitle = PRODUCT_NAME, title }: HeadProps) => {
  const router = useRouter();
  const metadata = buildBrandMetadata({ assetBase: router?.basePath || '' });

  return (
    <NextHead>
      <link href={metadata.fallbackIcon.href} key="mageml-favicon-fallback" rel="icon" sizes="any" />
      <link href={metadata.icon.href} key="mageml-favicon" rel="icon" type="image/svg+xml" />

      <title>{title ? `${title} | ${defaultTitle}` : defaultTitle}</title>

      {children}
    </NextHead>
  );
};

export default Head;
