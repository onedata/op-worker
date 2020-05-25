%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper module for luma_storage_users module.
%%% It encapsulates #luma_storage_user{} record.
%%%
%%% This record has 2 fields:
%%%  * storage_credentials - this is context of user (helpers:user_ctx())
%%%    passed to helper to perform operations on storage as a given user.
%%%  * display_uid - this field is used to display owner of a file (UID)
%%%    in Oneclient.
%%%
%%% For more info please read the docs of luma.erl and
%%% luma_storage_users.erl modules.
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

-spec new(luma:onedata_user_map() | od_user:id()) -> user().
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
        idp = maps:get(<<"idp">>, OnedataUserMap),
        subject_id = maps:get(<<"subjectId">>, OnedataUserMap)
    };
new(UserId) when is_binary(UserId) ->
    #luma_onedata_user{onedata_user_id = UserId}.

-spec get_user_id(user()) -> od_user:id().
get_user_id(#luma_onedata_user{onedata_user_id = OnedataUserId}) ->
    OnedataUserId.