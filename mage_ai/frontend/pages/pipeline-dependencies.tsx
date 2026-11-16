import Dashboard from '@components/Dashboard';
import PipelineDependencies from '@components/PipelineDependencies';
import PrivateRoute from '@components/shared/PrivateRoute';
import Spacing from '@oracle/elements/Spacing';
import { PADDING_UNITS } from '@oracle/styles/units/spacing';

function PipelineDependenciesPage() {
  return (
    <Dashboard
      title="Pipeline dependencies"
      uuid="PipelineDependencies/index"
    >
      <Spacing p={PADDING_UNITS}>
        <PipelineDependencies />
      </Spacing>
    </Dashboard>
  );
}

PipelineDependenciesPage.getInitialProps = async () => ({});

export default PrivateRoute(PipelineDependenciesPage);
