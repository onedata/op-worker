%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides high level DB API for session cookies handling.
%% @end
%% ===================================================================
-module(dao_cookies).

-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include_lib("dao/include/couch_db.hrl").
-include_lib("dao/include/common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API - cluster state
-export([save_cookie/1, get_cookie/1, remove_cookie/1, list_expired_cookies/2]).


%% ===================================================================
%% API functions
%% ===================================================================

%% save_cookie/1
%% ====================================================================
%% @doc Saves a session cookie to DB. Argument should be either #session_cookie{} record
%% (if you want to save it as new document) <br/>
%% or #db_document{} that wraps #session_cookie{} if you want to update the cookie in DB.
%% @end
-spec save_cookie(Cookie :: cookie_info() | user_doc()) -> {ok, cookie()} | {error, any()} | no_return().
%% ====================================================================
save_cookie(#session_cookie{} = Cookie) ->
    save_cookie(#db_document{record = Cookie});

save_cookie(#db_document{record = #session_cookie{}, uuid = UUID} = CookieDoc) when is_binary(UUID), UUID =/= <<"">> ->
    save_cookie(CookieDoc#db_document{uuid = binary_to_list(UUID)});

save_cookie(#db_document{record = #session_cookie{}, uuid = UUID} = CookieDoc) when is_list(UUID), UUID =/= "" ->
    dao_external:set_db(?COOKIES_DB_NAME),
    dao_records:save_record(CookieDoc);

save_cookie(#db_document{record = #session_cookie{}} = CookieDoc) ->
    dao_external:set_db(?COOKIES_DB_NAME),
    dao_records:save_record(CookieDoc#db_document{}).


%% get_cookie/1
%% ====================================================================
%% @doc Gets a session cookie from DB by its UUID. UUID should be the same as cookie value.
%% Non-error return value is always {ok, #db_document{record = #session_cookie}.
%% @end
-spec get_cookie(UUID :: cookie() | binary()) -> {ok, cookie_doc()} | {error, any()} | no_return().
%% ====================================================================
get_cookie(UUID) when is_binary(UUID) ->
    get_cookie(binary_to_list(UUID));

get_cookie(UUID) ->
    dao_external:set_db(?COOKIES_DB_NAME),
    dao_records:get_record(UUID).


%% remove_cookie/1
%% ====================================================================
%% @doc Gets a session cookie from DB by its UUID. UUID should be the same as cookie value.
%% Non-error return value is always {ok, #db_document{record = #session_cookie}.
%% @end
-spec remove_cookie(UUID :: cookie() | binary()) -> ok | {error, any()} | no_return().
%% ====================================================================
remove_cookie(UUID) when is_binary(UUID) ->
    remove_cookie(binary_to_list(UUID));

remove_cookie(UUID) ->
    dao_external:set_db(?COOKIES_DB_NAME),
    dao_records:remove_record(UUID).


%% list_expired_cookies/2
%% ====================================================================
%% @doc Lists all session cookies that have expired based on current time.
%% @end
-spec list_expired_cookies(Limit :: integer(), Offset::integer()) -> {ok, [cookie()]} | no_return().
%% ====================================================================
list_expired_cookies(Limit, Offset) ->
    StartKey = 0,
    {Megaseconds, Seconds, _} = now(),
    EndKey = Megaseconds * 1000000 + Seconds,
    QueryArgs = #view_query_args{start_key = StartKey, end_key = EndKey, include_docs = true, limit = Limit, skip = Offset},
    case dao_records:list_records(?COOKIES_BY_EXPIRED_BEFORE_VIEW, QueryArgs) of
        {ok, #view_result{rows = Rows}} ->
            {ok, [UUID || #view_row{doc = #db_document{uuid = UUID}} <- Rows]};
        Data ->
            ?error("Invalid session cookies view response: ~p", [Data]),
            throw({inavlid_data, Data})
    end.
