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
%%%   *  onedata_user_id  - which stores od_user:id(),
%%%   *  idp - which stores id of an external identity provider,
%%%   *  subject_id -  which stores id of the user, understood by
%%%      the idp.
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

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/storage/luma/external_luma.hrl").

%% API
-export([new/1, get_user_id/1]).

-record(luma_onedata_user, {
    onedata_user_id :: od_user:id(),
    idp :: undefined | idp(),
    subject_id :: undefined | subject_id()
}).

-type user() ::  #luma_onedata_user{}.
-type idp() ::  binary().
-type subject_id() ::  subject_id().

-export_type([user/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(external_reverse_luma:onedata_user() | od_user:id()) -> user().
new(OnedataUserMap = #{<<"mappingScheme">> := ?ONEDATA_USER_SCHEME}) ->
    #luma_onedata_user{
        onedata_user_id = maps:get(<<"onedataUserId">>, OnedataUserMap)
    };
new(OnedataUserMap = #{<<"mappingScheme">> := ?IDP_USER_SCHEME}) ->
    Idp = maps:get(<<"idp">>, OnedataUserMap),
    SubjectId = maps:get(<<"subjectId">>, OnedataUserMap),
    {ok, UserId} = provider_logic:map_idp_user_to_onedata(Idp, SubjectId),
    #luma_onedata_user{
        onedata_user_id = UserId,
        idp = Idp,
        subject_id = SubjectId
    };
new(UserId) when is_binary(UserId) ->
    #luma_onedata_user{onedata_user_id = UserId}.

-spec get_user_id(user()) -> od_user:id().
get_user_id(#luma_onedata_user{onedata_user_id = OnedataUserId}) ->
    OnedataUserId.