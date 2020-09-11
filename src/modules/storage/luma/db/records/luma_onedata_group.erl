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
%%%   * onedata_group_id - which stores od_group:id(),
%%%   * idp - id of an external identity provider,
%%%   * idp_entitlement - id of the group, understood by
%%%     the idp,
%%%   * mapping_scheme - name of scheme used to represent group identity.
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

-behaviour(luma_db_record).

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/luma/luma.hrl").

%% API
-export([new/1, get_group_id/1]).

%% luma_db_record callbacks
-export([to_json/1, from_json/1]).

-record(luma_onedata_group, {
    onedata_group_id :: od_group:id(),
    idp :: undefined | idp(),
    idp_entitlement :: undefined | idp_entitlement(),
    mapping_scheme :: binary() % ?ONEDATA_GROUP_SCHEME | ?IDP_ENTITLEMENT_SCHEME,
}).

-type group() ::  #luma_onedata_group{}.
-type idp() ::  binary().
-type idp_entitlement() ::  idp_entitlement().
-type group_map() :: json_utils:json_map().
%% structure of a group_map() is presented below
%% #{
%%      <<"mappingScheme">> => ?ONEDATA_GROUP_SCHEME | ?IDP_ENTITLEMENT_SCHEME,
%%
%%      % in case of ?ONEDATA_GROUP_SCHEME
%%      <<"onedataGroupId">> => binary()
%%
%%      % in case of ?IDP_ENTITLEMENT_SCHEME
%%      <<"idp">> => binary(),
%%      <<"idpEntitlement">> => binary()
%% }

-export_type([group/0, group_map/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(group_map() | od_group:id()) -> group().
new(OnedataGroupMap = #{<<"mappingScheme">> := ?ONEDATA_GROUP_SCHEME}) ->
    #luma_onedata_group{
        onedata_group_id = maps:get(<<"onedataGroupId">>, OnedataGroupMap),
        mapping_scheme = ?ONEDATA_GROUP_SCHEME
    };
new(OnedataGroupMap = #{<<"mappingScheme">> := ?IDP_ENTITLEMENT_SCHEME}) ->
    Idp = maps:get(<<"idp">>, OnedataGroupMap),
    IdpEntitlement = maps:get(<<"idpEntitlement">>, OnedataGroupMap),
    {ok, GroupId} = provider_logic:map_idp_group_to_onedata(Idp, IdpEntitlement),
    #luma_onedata_group{
        onedata_group_id = GroupId,
        idp = Idp,
        idp_entitlement = IdpEntitlement,
        mapping_scheme = ?IDP_ENTITLEMENT_SCHEME
    };
new(GroupId) when is_binary(GroupId) ->
    #luma_onedata_group{
        onedata_group_id = GroupId,
        mapping_scheme = ?ONEDATA_GROUP_SCHEME
    }.

-spec get_group_id(group()) -> od_group:id().
get_group_id(#luma_onedata_group{onedata_group_id = OnedataGroupId}) ->
    OnedataGroupId.

%%%===================================================================
%%% luma_db_record callbacks
%%%===================================================================

-spec to_json(group()) -> group_map().
to_json(#luma_onedata_group{
    onedata_group_id = OnedataGroupId,
    idp = Idp,
    idp_entitlement = IdpEntitlement,
    mapping_scheme = MappingScheme
}) ->
    #{
        <<"onedataGroupId">> => OnedataGroupId,
        <<"idp">> => utils:undefined_to_null(Idp),
        <<"idpEntitlement">> => utils:undefined_to_null(IdpEntitlement),
        <<"mappingScheme">> => MappingScheme
    }.

-spec from_json(group_map()) -> group().
from_json(GroupJson) ->
    #luma_onedata_group{
        onedata_group_id = maps:get(<<"onedataGroupId">>, GroupJson),
        idp = utils:null_to_undefined(maps:get(<<"idp">>, GroupJson, undefined)),
        idp_entitlement = utils:null_to_undefined(maps:get(<<"idpEntitlement">>, GroupJson, undefined)),
        mapping_scheme = maps:get(<<"mappingScheme">>, GroupJson)
    }.