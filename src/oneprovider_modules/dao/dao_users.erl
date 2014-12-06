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

-include_lib("oneprovider_modules/dao/dao.hrl").
-include_lib("dao/include/dao_helper.hrl").
-include_lib("oneprovider_modules/dao/dao_types.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").

%% ===================================================================
%% API functions
%% ===================================================================
-export([save_user/1, remove_user/1, exist_user/1, get_user/1, list_users/2,
    get_files_number/2, get_files_size/1, update_files_size/0, save_quota/1, remove_quota/1, get_quota/1]).


%% save_user/1
%% ====================================================================
%% @doc Saves user to DB. Argument should be either #user{} record
%% (if you want to save it as new document) <br/>
%% or #db_document{} that wraps #user{} if you want to update descriptor in DB. <br/>
%% See {@link dao_records:save_record/1} and {@link dao_records:get_record/1} for more details about #db_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec save_user(User :: user_info() | user_doc()) -> {ok, user()} | {error, any()} | no_return().
%% ====================================================================
save_user(#user{} = User) ->
    save_user(#db_document{record = User});
save_user(#db_document{record = #user{}, uuid = UUID} = UserDoc) when is_list(UUID), UUID =/= "" ->
    clear_all_data_from_cache(UUID),

    dao_external:set_db(?USERS_DB_NAME),
    dao_records:save_record(UserDoc);
save_user(#db_document{record = #user{}} = UserDoc) ->
    QueryArgs = #view_query_args{start_key = integer_to_binary(?HIGHEST_USER_ID), end_key = integer_to_binary(0),
        include_docs = false, limit = 1, direction = rev},
    NewUUID =
        case dao_records:list_records(?USER_BY_UID_VIEW, QueryArgs) of
            {ok, #view_result{rows = [#view_row{id = MaxUID} | _]}} ->
                integer_to_list(max(list_to_integer(MaxUID) + 1, ?LOWEST_USER_ID));
            {ok, #view_result{rows = []}} ->
                integer_to_list(?LOWEST_USER_ID);
            Other ->
                ?error("Invalid view response: ~p", [Other]),
                throw(invalid_data)   
        end,

    dao_external:set_db(?USERS_DB_NAME),
    dao_records:save_record(UserDoc#db_document{uuid = NewUUID}).


%% remove_user/1
%% ====================================================================
%% @doc Removes user from DB by login, e-mail, uuid or dn.
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec remove_user(Key :: user_key()) -> {error, any()} | no_return().
%% ====================================================================
remove_user(Key) ->
    {ok, FDoc} = get_user(Key),
    clear_all_data_from_cache(FDoc#db_document.uuid),
    dao_external:set_db(?USERS_DB_NAME),
    dao_records:remove_record(FDoc#db_document.uuid).

%% exist_user/1
%% ====================================================================
%% @doc Checks whether user exists in DB. Arguments should be login, e-mail, uuid or dn.
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec exist_user(Key :: user_key()) -> {ok, true | false} | {error, any()}.
%% ====================================================================
exist_user(Key) ->
    case ets:lookup(users_cache, Key) of
        [] -> exist_user_in_db(Key);
        [{_, _Ans}] -> {ok, true}
    end.

%% get_user/1
%% ====================================================================
%% @doc Gets user from DB by login, e-mail, uuid or dn.
%% Non-error return value is always {ok, #db_document{record = #user} | [#db_document{record = #user}]}.
%% See {@link dao_records:save_record/1} and {@link dao_records:get_record/1} for more details about #db_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec get_user(Key :: user_key()) -> {ok, user_doc() | [user_doc()]} | {error, any()} | no_return().
%% ====================================================================
get_user(Key) ->
    case ets:lookup(users_cache, Key) of
        [] -> %% Cached document not found. Fetch it from DB and save in cache
            DBAns = get_user_from_db(Key),
            case DBAns of
                {ok, #db_document{} = Doc} ->
                    ets:insert(users_cache, {Key, Doc}),
                    DocKey = Doc#db_document.uuid,
                    case ets:lookup(users_cache, {key_info, DocKey}) of
                        [] ->
                            ets:insert(users_cache, {{key_info, DocKey}, [Key]});
                        [{_, TmpInfo}] ->
                            ets:insert(users_cache, {{key_info, DocKey}, [Key | TmpInfo]})
                    end,
                    {ok, Doc};
                Other -> Other
            end;
        [{_, Ans}] -> %% Return document from cache
            {ok, Ans}
    end.

%% list_users/2
%% ====================================================================
%% @doc Lists N users from DB, starting from Offset. <br/>
%% Non-error return value is always {ok, [#db_document{record = #user}]}.
%% See {@link dao_records:save_record/1} and {@link dao_records:get_record/1} for more details about #db_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec list_users(N :: pos_integer(), Offset :: non_neg_integer()) ->
    {ok, DocList :: list(user_doc())} |
    no_return().
%% ====================================================================
list_users(N, Offset) ->
	dao_external:set_db(?USERS_DB_NAME),

    QueryArgs = #view_query_args{include_docs = true, limit = N, skip = Offset, inclusive_end = false},

    GetUser =
        fun(#view_row{doc = UserDoc}) ->
            UserDoc;
            (Other) ->
                ?error("Invalid row in view response: ~p", [Other]),
                throw(invalid_data)
        end,

    case dao_records:list_records(?USER_BY_LOGIN_VIEW, QueryArgs) of
        {ok, #view_result{rows = FDoc}} ->
            {ok, lists:map(GetUser, FDoc)};
        Other ->
            ?error("Invalid view response: ~p", [Other]),
            throw(invalid_data)
    end.

%% exist_user_in_db/1
%% ====================================================================
%% @doc Checks whether user exists in DB. Arguments should be login, e-mail, uuid or dn.
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec exist_user_in_db(Key :: user_key()) -> {ok, true | false} | {error, any()}.
%% ====================================================================
exist_user_in_db({uuid, "0"}) ->
    {ok, true};
exist_user_in_db({uuid, UUID}) ->
    dao_external:set_db(?USERS_DB_NAME),
    dao_records:exist_record(UUID);
exist_user_in_db({Key, Value}) ->
    dao_external:set_db(?USERS_DB_NAME),
    {View, QueryArgs} = case Key of
                            login ->
                                {?USER_BY_LOGIN_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}};
                            email ->
                                {?USER_BY_EMAIL_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}};
                            dn ->
                                {?USER_BY_DN_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}};
                            unverified_dn ->
                                {?USER_BY_UNVERIFIED_DN_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}};
                            global_id ->
                                {?USER_BY_GLOBAL_ID_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}};
                            name ->
                                {?USER_BY_NAME_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}}
                        end,
    case dao_records:list_records(View, QueryArgs) of
        {ok, #view_result{rows = [#view_row{doc = _FDoc} | _Tail]}} ->
            {ok, true};
        {ok, #view_result{rows = []}} ->
            {ok, false};
        Other -> Other
    end.

%% get_user_from_db/1
%% ====================================================================
%% @doc Gets user from DB by login, e-mail, uuid or dn.
%% Non-error return value is {ok, #db_document{record = #user}, or {ok, [#db_document{record = #user}]} when we query by name.
%% See {@link dao_records:save_record/1} and {@link dao_records:get_record/1} for more details about #db_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec get_user_from_db(Key :: user_key()) -> {ok, user_doc() | [user_doc()]} | {error, any()} | no_return().
%% ====================================================================
get_user_from_db({uuid, "cluster_uid"}) ->
    get_user_from_db({uuid, "0"});
get_user_from_db({uuid, "0"}) ->
    {ok, #db_document{uuid = "0", record = #user{logins = [#id_token_login{provider_id = internal, login = "root"}], name = "root"}}}; %% Return virtual "root" user
get_user_from_db({uuid, UUID}) ->
    dao_external:set_db(?USERS_DB_NAME),
    dao_records:get_record(UUID);

get_user_from_db({Key, Value}) ->
    dao_external:set_db(?USERS_DB_NAME),

    {View, QueryArgs} = case Key of
                            login ->
                                {?USER_BY_LOGIN_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}};
                            email ->
                                {?USER_BY_EMAIL_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}};
                            dn ->
                                {?USER_BY_DN_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}};
                            unverified_dn ->
                                {?USER_BY_UNVERIFIED_DN_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}};
                            global_id ->
                                {?USER_BY_GLOBAL_ID_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}};
                            name ->
                                {?USER_BY_NAME_VIEW, #view_query_args{keys =
                                [dao_helper:name(Value)], include_docs = true}}
                        end,

    case dao_records:list_records(View, QueryArgs) of
        {ok, #view_result{rows = []}} ->
            %% ?warning("User by ~p: ~p not found", [Key, Value]),
            throw(user_not_found);
        {ok, #view_result{rows = AllRows}} when Key == name ->
            {ok, [Doc || #view_row{doc = Doc} <- AllRows]};
        {ok, #view_result{rows = [#view_row{doc = FDoc}]}} ->
            {ok, FDoc};
        {ok, #view_result{rows = [#view_row{doc = FDoc} | Tail] = AllRows}} ->
            case length(lists:usort(AllRows)) of
                Count when Count > 1 ->
                    ?warning("User ~p is duplicated. Returning first copy. Others: ~p", [FDoc#db_document.record#user.logins, Tail]);
                _ -> ok
            end,
            {ok, FDoc};
        Other ->
            ?error("Invalid view response: ~p", [Other]),
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
  dao_external:set_db(?FILES_DB_NAME),
  View = case Type of user -> ?USER_FILES_NUMBER_VIEW; group -> ?GROUP_FILES_NUMBER_VIEW end,
  QueryArgs = #view_query_args{keys = [dao_helper:name(UUID)], include_docs = false, group_level = 1, view_type = reduce, stale = update_after},

    case dao_records:list_records(View, QueryArgs) of
        {ok, #view_result{rows = [#view_row{value = Sum}]}} ->
            {ok, Sum};
        {ok, #view_result{rows = []}} ->
            ?error("Number of files of ~p ~p not found", [Type, UUID]),
            throw(files_number_not_found);
        {ok, #view_result{rows = [#view_row{value = Sum} | Tail] = AllRows}} ->
            case length(lists:usort(AllRows)) of
                Count when Count > 1 ->
                    ?warning("To many rows in response during files number finding for ~p ~p. Others: ~p", [Type, UUID, Tail]);
                _ -> ok
            end,
            {ok, Sum};
        Other ->
            ?error("Invalid view response: ~p", [Other]),
            throw(invalid_data)
    end.

%% get_files_size/1
%% ====================================================================
%% @doc Returns size of user's files
%% @end
-spec get_files_size(UUID :: uuid()) -> {ok, non_neg_integer()} | {error, any()} | no_return().
%% ====================================================================
get_files_size(UUID) ->
  dao_external:set_db(?FILES_DB_NAME),
  QueryArgs = #view_query_args{keys = [dao_helper:name(UUID)], include_docs = false, group_level = 1, view_type = reduce, stale = update_after},

    case dao_records:list_records(?USER_FILES_SIZE_VIEW, QueryArgs) of
        {ok, #view_result{rows = [#view_row{value = Sum}]}} ->
            {ok, Sum};
        {ok, #view_result{rows = []}} ->
            ?error("Size of files of ~p not found", [UUID]),
            throw(files_size_not_found);
        {ok, #view_result{rows = [#view_row{value = Sum} | Tail] = AllRows}} ->
            case length(lists:usort(AllRows)) of
                Count when Count > 1 ->
                    ?warning("To many rows in response during files size finding for ~p. Others: ~p", [UUID, Tail]);
                _ -> ok
            end,
            {ok, Sum};
        Other ->
            ?error("Invalid view response: ~p", [Other]),
            throw(invalid_data)
    end.

%% update_files_size/1
%% ====================================================================
%% @doc Updates counting users' files sizes view in db
%% @end
-spec update_files_size() -> ok | {error, any()}.
%% ====================================================================
update_files_size() ->
  dao_external:set_db(?FILES_DB_NAME),
  QueryArgs = #view_query_args{keys = [undefined], include_docs = false, group_level = 1, view_type = reduce},

  case dao_records:list_records(?USER_FILES_SIZE_VIEW, QueryArgs) of
    {ok, _} -> ok;
    Other ->
      ?error("Invalid view response: ~p", [Other]),
      throw(invalid_data)
  end.

%% clear_all_data_from_cache/1
%% ====================================================================
%% @doc Deletes all data connected with user from user caches at all nodes
-spec clear_all_data_from_cache(DocKey :: string()) -> ok.
%% ====================================================================
clear_all_data_from_cache(DocKey) ->
    case ets:lookup(users_cache, {key_info, DocKey}) of
        [] ->
            ok;
        [{_, KeysList}] ->
            lists:foreach(fun(Key) -> ets:delete(users_cache, Key) end, KeysList),
            ets:delete(users_cache, {key_info, DocKey}),
            case worker_host:clear_cache({users_cache, [{key_info, DocKey} | KeysList]}) of
                ok -> ok;
                Error -> throw({error_during_global_cache_clearing, Error})
            end
    end.

%% save_quota/1
%% ====================================================================
%% @doc Saves users' quota to DB. Argument should be either #quota{} record
%% (if you want to save it as new document) <br/>
%% or #db_document{} that wraps #quota{} if you want to update descriptor in DB. <br/>
%% See {@link dao_records:save_record/1} and {@link dao_records:get_record/1} for more details about #db_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec save_quota(Quota :: quota_info() | quota_doc()) -> {ok, quota()} | {error, any()} | no_return().
%% ====================================================================
save_quota(#quota{} = Quota) ->
    save_quota(#db_document{record = Quota});
save_quota(#db_document{} = QuotaDoc) ->
  dao_external:set_db(?USERS_DB_NAME),
  dao_records:save_record(QuotaDoc).

%% remove_quota/1
%% ====================================================================
%% @doc Removes users' quota from DB by uuid.
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec remove_quota(UUID :: uuid()) -> {error, any()} | no_return().
%% ====================================================================
remove_quota(UUID) ->
  dao_external:set_db(?USERS_DB_NAME),
  dao_records:remove_record(UUID).

%% get_quota/1
%% ====================================================================
%% @doc Gets users' quota from DB by uuid.
%% Non-error return value is always {ok, #db_document{record = #quota}.
%% See {@link dao_records:save_record/1} and {@link dao_records:get_record/1} for more details about #db_document{} wrapper.<br/>
%% Should not be used directly, use {@link dao_worker:handle/2} instead (See {@link dao_worker:handle/2} for more details).
%% @end
-spec get_quota(UUID :: uuid()) -> {ok, quota_doc()} | {error, any()} | no_return().
%% ====================================================================
get_quota(UUID) ->
  dao_external:set_db(?USERS_DB_NAME),
  {ok, DefaultQuotaSize} = application:get_env(?APP_Name, default_quota),

  %% we want to be able to have special value in db for default quota in order to control default quota size via config in default.yml
  %% here we are replacing DEFAULT_QUOTA_DB_TAG with value configured in default.yml
  case dao_records:get_record(UUID) of
    {ok, #db_document{record = #quota{size = Size} = Quota} = QuotaDoc} when Size =:= ?DEFAULT_QUOTA_DB_TAG ->
      NewQuota = Quota#quota{size = DefaultQuotaSize},
      {ok, QuotaDoc#db_document{record = NewQuota}};
    Res -> Res
  end.
