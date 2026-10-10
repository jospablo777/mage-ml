from typing import Dict, Optional

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.data_preparation import contracts
from mage_ai.settings.repo import get_repo_path


def _first(query: Dict, key: str) -> Optional[str]:
    value = (query or {}).get(key)
    if isinstance(value, list):
        value = value[0] if value else None
    return value if isinstance(value, str) and value else None


def _block(repo_path: str, pipeline_uuid: Optional[str], block_uuid: Optional[str]):
    from mage_ai.data_preparation.models.pipeline import Pipeline

    if not pipeline_uuid or not block_uuid:
        return None
    pipeline = Pipeline.get(pipeline_uuid, repo_path=repo_path, check_if_exists=True)
    block = pipeline.get_block(block_uuid) if pipeline else None
    if block is None:
        raise ApiError(dict(code=404, message='The block does not exist.'))
    return block


class DataContractResource(GenericResource):
    """
    The project's data contracts (contracts/<name>.yaml): GET /api/data_contracts lists
    them; POST drafts one from a block's stored output and saves it; GET
    /api/data_contracts/<name>?pipeline_uuid=&block_uuid= returns the contract with the
    block's last notebook report.
    """

    @classmethod
    def collection(cls, query, meta, user, **kwargs):
        return cls.build_result_set(
            [dict(c, id=c['name']) for c in contracts.list_contracts(get_repo_path(user=user))],
            user,
            **kwargs,
        )

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'DataContractResource':
        repo_path = get_repo_path(user=user)
        block = _block(repo_path, payload.get('pipeline_uuid'), payload.get('block_uuid'))
        if block is None:
            raise ApiError(dict(code=400, message='A contract is drafted from a block output.'))
        name = payload.get('name') or block.uuid.replace('/', '_')
        if not isinstance(name, str) or not contracts._NAME.match(name):
            raise ApiError(dict(code=400, message=f'{name!r} is not a valid contract name.'))
        path = contracts.contracts_dir(repo_path) / f'{name}.yaml'
        if path.exists() or path.with_suffix('.yml').exists():
            raise ApiError(dict(code=409, message=f'Contract {name} exists already.'))
        try:
            document = contracts.draft(block, name)
        except contracts.ContractError as error:
            raise ApiError(dict(code=400, message=str(error)))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(document)
        return cls(dict(
            content=document,
            file_path=f'{contracts.CONTRACTS_FOLDER}/{path.name}',
            id=name,
            name=name,
        ), user, **kwargs)

    @classmethod
    def member(cls, pk, user, **kwargs) -> 'DataContractResource':
        repo_path = get_repo_path(user=user)
        try:
            path = contracts.contract_path(pk, repo_path)
        except contracts.ContractError as error:
            raise ApiError(dict(code=404, message=str(error)))
        model = dict(
            content=path.read_text(),
            file_path=f'{contracts.CONTRACTS_FOLDER}/{path.name}',
            id=pk,
            name=pk,
        )
        try:
            contract = contracts.load_contract(pk, repo_path)
            model.update(
                description=contract.get('description'),
                owner=contract.get('owner'),
                version=contract.get('version'),
            )
        except contracts.ContractError as error:
            model['error'] = str(error)
        query = kwargs.get('query') or {}
        block = _block(repo_path, _first(query, 'pipeline_uuid'), _first(query, 'block_uuid'))
        if block is not None:
            model['report'] = contracts.read_report(
                block, execution_partition=_first(query, 'execution_partition'),
            )
        return cls(model, user, **kwargs)
