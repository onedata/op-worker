%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper module for luma_onedata_users module.
%%% It encapsulates #luma_onedata_user{} record.
%%% It allows to uniquely identify a Onedata user.
%%%
%%% This record has 3 fields:
%%%   * onedata_user_id  - od_user:id(),
%%%   * idp - id of an external identity provider,
%%%   * subject_id - id of the user, understood by
%%%     the idp,
%%%   * mapping_scheme - name of scheme used to represent user identity.
%%% 
%%% If onedata_user_id is missing, a call to Onezone is performed to
%%% compute it basing on idp and subject_id.
%%%
%%% idp and subject_id may be undefined.
%%%
%%% For more info please read the docs of luma.erl and
%%% luma_onedata_users.erl modules.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_onedata_user).
-author("Jakub Kudzia").

-behaviour(luma_db_record).

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/luma/luma.hrl").

%% API
-export([new/1, get_user_id/1]).

%% luma_db_record callbacks
-export([to_json/1, from_json/1, update/2]).

-record(luma_onedata_user, {
    onedata_user_id :: od_user:id(),
    idp :: undefined | idp(),
    subject_id :: undefined | subject_id(),
    mapping_scheme :: binary() % ?ONEDATA_USER_SCHEME | ?IDP_USER_SCHEME,
}).

-type user() ::  #luma_onedata_user{}.
-type idp() ::  binary().
-type subject_id() ::  subject_id().
-type user_map() :: json_utils:json_map().
%% structure of a user_map() is presented below
%% #{
%%      <<"mappingScheme">> => ?ONEDATA_USER_SCHEME | ?IDP_USER_SCHEME,
%%
%%      % in case of ?ONEDATA_USER_SCHEME
%%      <<"onedataUserId">> => binary()
%%
%%      % in case of ?IDP_USER_SCHEME
%%      <<"idp">> => binary(),
%%      <<"subjectId">> => binary()
%% }


-export_type([user/0, user_map/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(user_map() | od_user:id()) -> user().
new(OnedataUserMap = #{<<"mappingScheme">> := ?ONEDATA_USER_SCHEME}) ->
    #luma_onedata_user{
        onedata_user_id = maps:get(<<"onedataUserId">>, OnedataUserMap),
        mapping_scheme = ?ONEDATA_USER_SCHEME
    };
new(OnedataUserMap = #{<<"mappingScheme">> := ?IDP_USER_SCHEME}) ->
    Idp = maps:get(<<"idp">>, OnedataUserMap),
    SubjectId = maps:get(<<"subjectId">>, OnedataUserMap),
    {ok, UserId} = provider_logic:map_idp_user_to_onedata(Idp, SubjectId),
    #luma_onedata_user{
        onedata_user_id = UserId,
        idp = Idp,
        subject_id = SubjectId,
        mapping_scheme = ?IDP_USER_SCHEME
    };
new(UserId) when is_binary(UserId) ->
    #luma_onedata_user{
        onedata_user_id = UserId,
        mapping_scheme = ?ONEDATA_USER_SCHEME
    }.

-spec get_user_id(user()) -> od_user:id().
get_user_id(#luma_onedata_user{onedata_user_id = OnedataUserId}) ->
    OnedataUserId.

%%%===================================================================
%%% luma_db_record callbacks
%%%===================================================================

-spec to_json(user()) -> user_map().
to_json(#luma_onedata_user{
    onedata_user_id = OnedataUserId,
    idp = Idp,
    subject_id = SubjectId,
    mapping_scheme = MappingScheme
}) ->
    #{
        <<"onedataUserId">> => OnedataUserId,
        <<"idp">> => utils:undefined_to_null(Idp),
        <<"subjectId">> => utils:undefined_to_null(SubjectId),
        <<"mappingScheme">> => MappingScheme
    }.

-spec from_json(user_map()) -> user().
from_json(UserJson) ->
    #luma_onedata_user{
        onedata_user_id = maps:get(<<"onedataUserId">>, UserJson),
        idp = utils:null_to_undefined(maps:get(<<"idp">>, UserJson, undefined)),
        subject_id = utils:null_to_undefined(maps:get(<<"subjectId">>, UserJson, undefined)),
        mapping_scheme = maps:get(<<"mappingScheme">>, UserJson)
    }.

-spec update(user(), luma_db:db_diff()) -> {ok, user()}.
update(LOU = #luma_onedata_user{
    onedata_user_id = UserId,
    idp = Idp,
    subject_id = SubjectId
}, Diff) ->
    NewRecord  = #luma_onedata_user{onedata_user_id = NewUserId} = new(Diff),
    case NewUserId =:= UserId of
        true ->
            {ok, LOU#luma_onedata_user{
                idp = utils:ensure_defined(NewRecord#luma_onedata_user.idp, undefined, Idp),
                subject_id =  utils:ensure_defined(NewRecord#luma_onedata_user.subject_id, undefined, SubjectId)
            }};
        false ->
            {ok, NewRecord}
    end.