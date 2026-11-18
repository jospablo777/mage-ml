import Dashboard from '@components/Dashboard';
import PrivateRoute from '@components/shared/PrivateRoute';
import Spacing from '@oracle/elements/Spacing';
import WorkerDetail from '@components/Workers/WorkerDetail';
import { PADDING_UNITS } from '@oracle/styles/units/spacing';

function WorkerPage({ worker }: { worker: string }) {
  return (
    <Dashboard
      title={worker}
      uuid="Workers/detail"
    >
      <Spacing p={PADDING_UNITS}>
        <WorkerDetail name={worker} />
      </Spacing>
    </Dashboard>
  );
}

WorkerPage.getInitialProps = async (ctx: any) => ({ worker: ctx.query.worker });

export default PrivateRoute(WorkerPage);
