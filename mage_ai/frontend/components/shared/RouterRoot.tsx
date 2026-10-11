import Router from 'next/router';
import { RouterContext } from 'next/dist/shared/lib/router-context.shared-runtime';

type RouterRootProps = {
  children: React.ReactNode;
};

// Components rendered with createRoot live outside the app's tree and its router context.
// Since Next 13, useRouter() throws there ("NextRouter was not mounted"), which broke every
// component that calls it, Button included. The singleton router always reads the current
// route, so it stays correct after navigation.
export default function RouterRoot({ children }: RouterRootProps) {
  return (
    // @ts-ignore: the singleton exposes the same fields and methods as the router instance
    <RouterContext.Provider value={Router}>
      {children}
    </RouterContext.Provider>
  );
}
