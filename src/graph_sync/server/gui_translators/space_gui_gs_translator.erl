%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles translation of middleware results concerning
%%% space entities into GUI GRAPH SYNC responses.
%%% @end
%%%-------------------------------------------------------------------
-module(space_gui_gs_translator).
-author("Bartosz Walkowicz").

-include("middleware/middleware.hrl").
-include("modules/dataset/archivisation_tree.hrl").

%% API
-export([
    translate_value/2,
    translate_resource/2
]).


%%%===================================================================
%%% API
%%%===================================================================

-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
translate_value(#gri{aspect = infer_accessible_eff_groups}, Result) ->
    Result;

translate_value(#gri{aspect = transfers}, #{<<"transfers">> := TransfersIds}) ->
    #{<<"list">> => lists:map(fun(TransferId) -> gri:serialize(#gri{
        type = op_transfer,
        id = TransferId,
        aspect = instance,
        scope = private
    }) end, TransfersIds)};

translate_value(#gri{aspect = transfers_active_channels}, ActiveChannels) ->
    ActiveChannels;

translate_value(#gri{aspect = {transfers_throughput_charts, _}}, Charts) ->
    Charts;

translate_value(#gri{aspect = evaluate_qos_expression}, Result) ->
    Result;

translate_value(#gri{aspect = datasets_details, scope = private}, {Datasets, IsLast}) ->
    dataset_gui_gs_translator:translate_datasets_details_list(Datasets, IsLast);

translate_value(#gri{
    aspect = atm_workflow_execution_summaries,
    scope = private
}, {AtmWorkflowExecutionSummaries, IsLast}) ->
    #{
        <<"list">> => lists:map(
            fun atm_workflow_execution_gui_gs_translator:translate_atm_workflow_execution_summary/1,
            AtmWorkflowExecutionSummaries
        ),
        <<"isLast">> => IsLast
    };

translate_value(#gri{aspect = dir_stats_service_state, scope = private}, Result) ->
    Result.


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{id = SpaceId, aspect = instance, scope = private}, Space) ->
    IsSpaceSupportedLocally = space_logic:is_supported(Space, oneprovider:get_id()),

    {DirIdsJson, PreferableWriteBlockSize} = case IsSpaceSupportedLocally of
        true ->
            RootDirGRI = gri:serialize(#gri{
                type = op_file,
                id = fslogic_file_id:spaceid_to_space_dir_guid(SpaceId),
                aspect = instance,
                scope = private
            }),
            {#{
                <<"rootDir">> => RootDirGRI,
                <<"trashDirId">> => file_id:pack_guid(?TRASH_DIR_UUID(SpaceId), SpaceId),
                <<"archivesDirId">> => file_id:pack_guid(?ARCHIVES_ROOT_DIR_UUID(SpaceId), SpaceId)
            }, file_upload_utils:get_preferable_write_block_size(SpaceId)};
        false ->
            {#{<<"rootDir">> => null}, undefined}
    end,

    ProvidersWithReadonlySupport = lists:filter(fun(ProviderId) ->
        space_logic:has_readonly_support_from(SpaceId, ProviderId)
    end, maps:keys(Space#od_space.storages_by_provider)),

    Result = DirIdsJson#{
        <<"name">> => Space#od_space.name,
        <<"effUserList">> => gri:serialize(#gri{
            type = op_space,
            id = SpaceId,
            aspect = eff_users,
            scope = private
        }),
        <<"effGroupList">> => gri:serialize(#gri{
            type = op_space,
            id = SpaceId,
            aspect = eff_groups,
            scope = private
        }),
        <<"providerList">> => gri:serialize(#gri{
            type = op_space,
            id = SpaceId,
            aspect = providers,
            scope = private
        }),
        <<"preferableWriteBlockSize">> => utils:undefined_to_null(PreferableWriteBlockSize),
        <<"providersWithReadonlySupport">> => ProvidersWithReadonlySupport
    },
    fun
        (?USER(Id)) -> 
            Result#{
                <<"currentUserEffPrivileges">> => maps:get(Id, Space#od_space.eff_users, []),
                <<"currentUserIsOwner">> => lists:member(Id, Space#od_space.owners)
            };
        (_) -> 
            Result
    end;
translate_resource(#gri{aspect = eff_users, scope = private}, Users) ->
    #{
        <<"list">> => lists:map(fun(UserId) ->
            gri:serialize(#gri{
                type = op_user,
                id = UserId,
                aspect = instance,
                scope = shared
            })
        end, maps:keys(Users))
    };
translate_resource(#gri{aspect = eff_groups, scope = private}, Groups) ->
    #{
        <<"list">> => lists:map(fun(GroupId) ->
            gri:serialize(#gri{
                type = op_group,
                id = GroupId,
                aspect = instance,
                scope = shared
            })
        end, maps:keys(Groups))
    };
translate_resource(#gri{aspect = providers, scope = private}, ProviderIds) ->
    #{
        <<"list">> => lists:map(fun(ProviderId) ->
            gri:serialize(#gri{
                type = op_provider,
                id = ProviderId,
                aspect = instance,
                scope = protected
            })
        end, ProviderIds)
    };
translate_resource(#gri{aspect = {view, _}, scope = private}, ViewInfo) ->
    ViewInfo;
translate_resource(#gri{aspect = available_qos_parameters}, QosParameters) ->
    #{
        <<"qosParameters">> => QosParameters
    }.
