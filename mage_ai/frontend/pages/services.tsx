import Dashboard from '@components/Dashboard';
import PrivateRoute from '@components/shared/PrivateRoute';
import Services from '@components/Services';
import Spacing from '@oracle/elements/Spacing';
import { PADDING_UNITS } from '@oracle/styles/units/spacing';

function ServicesPage() {
  return (
    <Dashboard
      title="Services"
      uuid="Services/index"
    >
      <Spacing p={PADDING_UNITS}>
        <Services />
      </Spacing>
    </Dashboard>
  );
}

ServicesPage.getInitialProps = async () => ({});

export default PrivateRoute(ServicesPage);
