%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides high level DB API for handling user documents.
%% @end
%% ===================================================================
-module(dao_users).

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_helper.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").

%% ===================================================================
%% API functions
%% ===================================================================
-export([save_user/1, remove_user/1, get_user/1, get_files_number/2]).


%% save_user/1
%% ====================================================================
%% @doc Saves user to DB. Argument should be either #user{} record
%% (if you want to save it as new document) <br/>
%% or #veil_document{} that wraps #user{} if you want to update descriptor in DB. <br/>
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec save_user(User :: user_info() | user_doc()) -> {ok, user()} | {error, any()} | no_return().
%% ====================================================================
save_user(#user{} = User) ->
    save_user(#veil_document{record = User});
save_user(#veil_document{record = #user{}, uuid = UUID} = UserDoc) when is_list(UUID), UUID =/= "" ->
    clear_all_data_from_cache(UserDoc),

    dao:set_db(?USERS_DB_NAME),
    dao:save_record(UserDoc);
save_user(#veil_document{record = #user{}} = UserDoc) ->
    QueryArgs = #view_query_args{start_key = integer_to_binary(?HIGHEST_USER_ID), end_key = integer_to_binary(0), 
                                 include_docs = false, limit = 1, direction = rev},
    NewUUID =
        case dao:list_records(?USER_BY_UID_VIEW, QueryArgs) of 
            {ok, #view_result{rows = [#view_row{id = MaxUID} | _]}} ->
                integer_to_list(max(list_to_integer(MaxUID) + 1, ?LOWEST_USER_ID));
            {ok, #view_result{rows = []}} ->
                integer_to_list(?LOWEST_USER_ID); 
            Other ->
                lager:error("Invalid view response: ~p", [Other]),
                throw(invalid_data)   
        end,

    dao:set_db(?USERS_DB_NAME),
    dao:save_record(UserDoc#veil_document{uuid = NewUUID}).


%% remove_user/1
%% ====================================================================
%% @doc Removes user from DB by login, e-mail, uuid or dn.
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec remove_user(Key:: {login, Login :: string()} | 
                        {email, Email :: string()} | 
                        {uuid, UUID :: uuid()} | 
                        {dn, DN :: string()}) -> 
    {error, any()} | no_return().
%% ====================================================================
remove_user(Key) ->
    {ok, FDoc} = get_user(Key),
    clear_all_data_from_cache(FDoc),
    dao:set_db(?USERS_DB_NAME),
    dao:remove_record(FDoc#veil_document.uuid).

clear_cache(Key) ->
  ets:delete(users_cache, Key),
  case worker_host:clear_cache({users_cache, Key}) of
    ok -> ok;
    Error -> throw({error_during_global_cache_clearing, Error})
  end.

clear_all_data_from_cache(UserDoc) ->
  Doc = UserDoc#veil_document.record,
  Caches = [{uuid, UserDoc#veil_document.uuid}, {login, Doc#user.login}],
  Caches2 = lists:foldl(fun(EMail, TmpAns) -> [{email, EMail} | TmpAns] end, Caches, Doc#user.email_list),
  Caches3 = lists:foldl(fun(DN, TmpAns) -> [{dn, DN} | TmpAns] end, Caches2, Doc#user.dn_list),
  clear_cache(Caches3).

%% get_user/1
%% ====================================================================
%% @doc Gets user from DB by login, e-mail, uuid or dn.
%% Non-error return value is always {ok, #veil_document{record = #user}.
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec get_user(Key::    {login, Login :: string()} |
{email, Email :: string()} |
{uuid, UUID :: uuid()} |
{dn, DN :: string()}) ->
  {ok, user_doc()} | {error, any()} | no_return().
%% ====================================================================
get_user(Key) ->
  case ets:lookup(users_cache, Key) of
    [] -> %% Cached document not found. Fetch it from DB and save in cache
      DBAns = get_user_from_db(Key),
      case DBAns of
        {ok, Doc} ->
          ets:insert(users_cache, {Key, Doc}),
          {ok, Doc};
        Other -> Other
      end;
    [{_, Ans}] -> %% Return document from cache
      {ok, Ans}
  end.

%% get_user_from_db/1
%% ====================================================================
%% @doc Gets user from DB by login, e-mail, uuid or dn.
%% Non-error return value is always {ok, #veil_document{record = #user}.
%% See {@link dao:save_record/1} and {@link dao:get_record/1} for more details about #veil_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao:handle/2} instead (See {@link dao:handle/2} for more details).
%% @end
-spec get_user_from_db(Key::    {login, Login :: string()} |
                        {email, Email :: string()} | 
                        {uuid, UUID :: uuid()} | 
                        {dn, DN :: string()}) -> 
    {ok, user_doc()} | {error, any()} | no_return().
%% ====================================================================
get_user_from_db({uuid, "0"}) ->
    {ok, #veil_document{uuid = "0", record = #user{login = "root", name = "root"}}}; %% Return virtual "root" user
get_user_from_db({uuid, UUID}) ->
    dao:set_db(?USERS_DB_NAME),
    dao:get_record(UUID);

get_user_from_db({Key, Value}) ->
    dao:set_db(?USERS_DB_NAME),

    {View, QueryArgs} = case Key of
        login -> 
            {?USER_BY_LOGIN_VIEW, #view_query_args{keys = 
                [dao_helper:name(Value)], include_docs = true}};
        email -> 
            {?USER_BY_EMAIL_VIEW, #view_query_args{keys = 
                [dao_helper:name(Value)], include_docs = true}};
        dn -> 
            {?USER_BY_DN_VIEW, #view_query_args{keys = 
                [dao_helper:name(Value)], include_docs = true}}
    end,

    case dao:list_records(View, QueryArgs) of
        {ok, #view_result{rows = [#view_row{doc = FDoc}]}} ->
            {ok, FDoc};
        {ok, #view_result{rows = []}} ->
            lager:error("User by ~p: ~p not found", [Key, Value]),
            throw(user_not_found);
        {ok, #view_result{rows = [#view_row{doc = FDoc} | Tail] = AllRows}} ->
            case length(lists:usort(AllRows)) of 
                Count when Count > 1 -> lager:warning("User ~p is duplicated. Returning first copy. Others: ~p", [FDoc#veil_document.record#user.login, Tail]);
                _ -> ok
            end,
            {ok, FDoc};
        Other ->
            lager:error("Invalid view response: ~p", [Other]),
            throw(invalid_data)
    end.

%% get_files_number/1
%% ====================================================================
%% @doc Returns number of user's / group's files
%% @end
    -spec get_files_number(user | group, UUID :: uuid()) -> Result when
Result :: {ok, Sum} | {error, any()} | no_return(),
Sum :: integer().
%% ====================================================================
get_files_number(Type, UUID) ->
  dao:set_db(?FILES_DB_NAME),
  View = case Type of user -> ?USER_FILES_NUMBER_VIEW; group -> ?GROUP_FILES_NUMBER_VIEW end,
  QueryArgs = #view_query_args{keys = [dao_helper:name(UUID)], include_docs = false, group_level = 1, view_type = reduce, stale = update_after},

  case dao:list_records(View, QueryArgs) of
    {ok, #view_result{rows = [#view_row{value = Sum}]}} ->
      {ok, Sum};
    {ok, #view_result{rows = []}} ->
      lager:error("Number of files of ~p ~p not found", [Type, UUID]),
      throw(files_number_not_found);
    {ok, #view_result{rows = [#view_row{value = Sum} | Tail] = AllRows}} ->
      case length(lists:usort(AllRows)) of
        Count when Count > 1 -> lager:warning("To many rows in response during files number finding for ~p ~p. Others: ~p", [Type, UUID, Tail]);
        _ -> ok
      end,
      {ok, Sum};
    Other ->
      lager:error("Invalid view response: ~p", [Other]),
      throw(invalid_data)
  end.