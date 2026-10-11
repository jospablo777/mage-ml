import Documentation from '@components/PipelineDetail/Documentation';
import PipelineDetailPage from '@components/PipelineDetailPage';
import PrivateRoute from '@components/shared/PrivateRoute';
import { PageNameEnum } from '@components/PipelineDetailPage/constants';

type PipelineDocumentationPageProps = {
  pipeline: {
    uuid: string;
  };
};

function PipelineDocumentationPage({ pipeline }: PipelineDocumentationPageProps) {
  return (
    <PipelineDetailPage
      breadcrumbs={[
        {
          label: () => 'Documentation',
        },
      ]}
      pageName={PageNameEnum.DOCUMENTATION}
      pipeline={pipeline}
      title={({ name }) => `${name} documentation`}
      uuid={`${PageNameEnum.DOCUMENTATION}_${pipeline?.uuid}`}
    >
      <Documentation pipelineUUID={pipeline?.uuid} />
    </PipelineDetailPage>
  );
}

PipelineDocumentationPage.getInitialProps = async (ctx: any) => {
  const { pipeline: pipelineUUID }: { pipeline: string } = ctx.query;

  return {
    pipeline: {
      uuid: pipelineUUID,
    },
  };
};

export default PrivateRoute(PipelineDocumentationPage);
