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

%% API
-export([
    translate_value/2,
    translate_resource/2
]).


%%%===================================================================
%%% API
%%%===================================================================


-spec translate_value(gri:gri(), Value :: term()) -> gs_protocol:data().
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
    Charts.


-spec translate_resource(gri:gri(), Data :: term()) ->
    gs_protocol:data() | fun((aai:auth()) -> gs_protocol:data()).
translate_resource(#gri{id = SpaceId, aspect = instance, scope = private}, Space) ->
    IsSpaceSupportedLocally = space_logic:is_supported(Space, oneprovider:get_id()),

    {RootDir, PreferableWriteBlockSize} = case IsSpaceSupportedLocally of
        true ->
            RootDirGRI = gri:serialize(#gri{
                type = op_file,
                id = fslogic_uuid:spaceid_to_space_dir_guid(SpaceId),
                aspect = instance,
                scope = private
            }),
            {RootDirGRI, file_upload_utils:get_preferable_write_block_size(SpaceId)};
        false ->
            {undefined, undefined}
    end,

    ProvidersWithReadonlySupport = lists:filter(fun(ProviderId) ->
        space_logic:has_readonly_support_from(SpaceId, ProviderId)
    end, maps:keys(Space#od_space.storages_by_provider)),

    Result = #{
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
        <<"shareList">> => gri:serialize(#gri{
            type = op_space,
            id = SpaceId,
            aspect = shares,
            scope = private
        }),
        <<"providerList">> => gri:serialize(#gri{
            type = op_space,
            id = SpaceId,
            aspect = providers,
            scope = private
        }),
        <<"rootDir">> => utils:undefined_to_null(RootDir),
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
translate_resource(#gri{aspect = shares, scope = private}, ShareIds) ->
    #{
        <<"list">> => lists:map(fun(ShareId) ->
            gri:serialize(#gri{
                type = op_share,
                id = ShareId,
                aspect = instance,
                scope = private
            })
        end, ShareIds)
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
