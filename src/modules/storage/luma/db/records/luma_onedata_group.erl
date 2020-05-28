%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper module for luma_onedata_groups module.
%%% It encapsulates #luma_onedata_group{} record.
%%% It allows to uniquely identify a Onedata group.
%%%
%%% This record has 3 fields:
%%%   *  onedata_group_id  - which stores od_group:id(),
%%%   *  idp - which stores id of an external identity provider,
%%%   *  idp_entitlement which stores id of the group, understood by
%%%      the idp.
%%%
%%% If onedata_group_id is missing, a call to Onezone is performed to
%%% compute it basing on idp and idp_entitlement.
%%%
%%% idp and idp_entitlement may be undefined.
%%%
%%% For more info please read the docs of luma.erl and
%%% luma_onedata_groups.erl modules.
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

-spec new(external_reverse_luma:onedata_group() | od_group:id()) -> group().
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
        idp = Idp,
        idp_entitlement = IdpEntitlement
    };
new(GroupId) when is_binary(GroupId) ->
    #luma_onedata_group{onedata_group_id = GroupId}.

-spec get_group_id(group()) -> od_group:id().
get_group_id(#luma_onedata_group{onedata_group_id = OnedataGroupId}) ->
    OnedataGroupId.