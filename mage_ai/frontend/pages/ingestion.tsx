import Dashboard from '@components/Dashboard';
import Ingestion from '@components/Ingestion';
import PrivateRoute from '@components/shared/PrivateRoute';
import Spacing from '@oracle/elements/Spacing';
import { PADDING_UNITS } from '@oracle/styles/units/spacing';

function IngestionPage() {
  return (
    <Dashboard
      title="Ingestion"
      uuid="Ingestion/index"
    >
      <Spacing p={PADDING_UNITS}>
        <Ingestion />
      </Spacing>
    </Dashboard>
  );
}

IngestionPage.getInitialProps = async () => ({});

export default PrivateRoute(IngestionPage);
