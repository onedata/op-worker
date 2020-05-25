%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper module for luma_storage_groups module.
%%% It encapsulates #luma_storage_group{} record.
%%%
%%% This record has 2 fields:
%%%  * storage_credentials - this is context of group (helpers:group_ctx())
%%%    passed to helper to perform operations on storage as a given group.
%%%  * display_uid - this field is used to display owner of a file (UID)
%%%    in Oneclient.
%%%
%%% For more info please read the docs of luma.erl and
%%% luma_storage_groups.erl modules.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_onedata_group).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/luma/external_luma.hrl").

%% API
-export([new/1, get_group_id/1]).

-record(luma_onedata_group, {
    onedata_group_id :: od_group:id(),
    idp :: undefined | idp(),
    idp_entitlement :: undefined | idp_entitlement()
}).

-type group() ::  #luma_onedata_group{}.
-type idp() ::  binary().
-type idp_entitlement() ::  idp_entitlement().

-export_type([group/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(luma:onedata_group_map() | od_group:id()) -> group().
new(OnedataGroupMap = #{<<"mappingScheme">> := ?ONEDATA_GROUP_SCHEME}) ->
    #luma_onedata_group{
        onedata_group_id = maps:get(<<"onedataGroupId">>, OnedataGroupMap)
    };
new(OnedataGroupMap = #{<<"mappingScheme">> := ?IDP_ENTITLEMENT_SCHEME}) ->
    Idp = maps:get(<<"idp">>, OnedataGroupMap),
    IdpEntitlement = maps:get(<<"idpEntitlement">>, OnedataGroupMap),
    {ok, GroupId} = provider_logic:map_idp_group_to_onedata(Idp, IdpEntitlement),
    #luma_onedata_group{
        onedata_group_id = GroupId,
        idp = maps:get(<<"idp">>, OnedataGroupMap),
        idp_entitlement = maps:get(<<"idpEntitlement">>, OnedataGroupMap)
    };
new(GroupId) when is_binary(GroupId) ->
    #luma_onedata_group{onedata_group_id = GroupId}.

-spec get_group_id(group()) -> od_group:id().
get_group_id(#luma_onedata_group{onedata_group_id = OnedataGroupId}) ->
    OnedataGroupId.