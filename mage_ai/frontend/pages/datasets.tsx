import Dashboard from '@components/Dashboard';
import Datasets from '@components/DatasetWorkspace';
import PrivateRoute from '@components/shared/PrivateRoute';
import Spacing from '@oracle/elements/Spacing';
import { PADDING_UNITS } from '@oracle/styles/units/spacing';

function DatasetsPage() {
  return (
    <Dashboard
      title="Datasets"
      uuid="Datasets/index"
    >
      <Spacing p={PADDING_UNITS}>
        <Datasets />
      </Spacing>
    </Dashboard>
  );
}

DatasetsPage.getInitialProps = async () => ({});

export default PrivateRoute(DatasetsPage);
