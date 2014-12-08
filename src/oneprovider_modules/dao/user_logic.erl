%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides methods for managing users in the system.
%% @end
%% ===================================================================

-module(user_logic).

-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("registered_names.hrl").
-include("oneprovider_modules/dao/dao_types.hrl").
-include_lib("oneprovider_modules/dao/dao.hrl").
-include_lib("oneprovider_modules/dao/dao_types.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_openid.hrl").


%% ====================================================================
%% API
%% ====================================================================
-export([sign_in/4, create_user/6, create_user/9, get_user/1, remove_user/1, list_all_users/0]).
-export([get_login/1, get_name/1, update_name/2, get_teams/1, update_teams/2, get_space_ids/1]).
-export([get_email_list/1, update_email_list/2, get_role/1, update_role/2, update_access_credentials/4]).
-export([get_dn_list/1, update_dn_list/2, get_unverified_dn_list/1, update_unverified_dn_list/2]).
-export([rdn_sequence_to_dn_string/1, extract_dn_from_cert/1, invert_dn_string/1]).
-export([shortname_to_oid_code/1, oid_code_to_shortname/1]).
-export([get_space_names/1, create_space_dir/1, get_spaces/1]).
-export([create_dirs_at_storage/2, create_dirs_at_storage/1]).
-export([get_quota/1, update_quota/2, get_files_size/2, quota_exceeded/2]).
-export([synchronize_spaces_info/2, synchronize_groups/2, create_partial_user/2, create_partial_user/3, get_login_with_uid/1]).


%% ====================================================================
%% API functions
%% ====================================================================


%% create_partial_user/2
%% ====================================================================
%% @doc Creates user based on data received from Global Registry for user with given GRUID.
%%      Spaces list shall contain at least one of user's spaces.
%% @end
-spec create_partial_user(GRUID :: binary(), [space_info()]) -> {ok, user_doc()} | {error, Reason :: term()}.
%% ====================================================================
create_partial_user(GRUID, Spaces) ->
  create_partial_user(GRUID, Spaces, []).

%% create_partial_user/3
%% ====================================================================
%% @doc Creates user based on data received from Global Registry for user with given GRUID.
%%      Spaces list shall contain at least one of user's spaces. Adds user to groups.
%% @end
-spec create_partial_user(GRUID :: binary(), [space_info()], Groups :: list()) -> {ok, user_doc()} | {error, Reason :: term()}.
%% ====================================================================
create_partial_user(GRUID, Spaces, Groups) ->
    [#space_info{space_id = SpaceId} | _] = Spaces,
    case gr_spaces:get_user_details(provider, SpaceId, utils:ensure_binary(GRUID)) of
        {ok, #user_details{name = Name0}} ->
            Login = openid_utils:get_user_login(GRUID),
            Name = unicode:characters_to_list(Name0),
            try user_logic:sign_in([{global_id, utils:ensure_list(GRUID)}, {name, Name}, {login, Login}, {groups, Groups}], <<>>, <<>>, 0) of
                {_, UserDoc} ->
                    {ok, UserDoc}
            catch
                _:Reason ->
                    ?error("Cannot create partial user ~p due to: ~p", [GRUID, Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            ?error("Cannot create partial user ~p due to: ~p", [GRUID, Reason]),
            {error, Reason}
    end.


%% sign_in/4
%% ====================================================================
%% @doc
%% This function should be called after a user has logged in via OpenID.
%% It looks the user up in database by login. If he is not there, it creates a proper document.
%% If the user already exists, synchronization is made between document
%% in the database and info received from OpenID provider.
%% @end
-spec sign_in(Proplist, AccessToken :: binary(),
    RefreshToken :: binary(),
    AccessExpirationTime :: non_neg_integer()) ->
    Result when
    Proplist :: list(),
    Result :: {string(), user_doc()} | no_return().
%% ====================================================================
sign_in(Proplist, AccessToken, RefreshToken, AccessExpirationTime) ->
    GlobalId = case proplists:get_value(global_id, Proplist, "") of
                   "" -> throw(no_global_id_specified);
                   GID -> GID
               end,
    Logins = proplists:get_value(logins, Proplist, []),
    Name = proplists:get_value(name, Proplist, ""),
    Teams = proplists:get_value(teams, Proplist, []),
    Emails = proplists:get_value(emails, Proplist, []),
    DnList = proplists:get_value(dn_list, Proplist, []),
    Groups = proplists:get_value(groups, Proplist, []),

    ?debug("Login with token: ~p", [AccessToken]),

    User = case get_user({global_id, GlobalId}) of
               {ok, ExistingUser} ->
                   User1 = synchronize_user_info(ExistingUser, Logins, Teams, Emails, DnList, AccessToken, RefreshToken, AccessExpirationTime),
                   synchronize_spaces_info(User1, AccessToken);
               {error, user_not_found} ->
                   case create_user(GlobalId, Logins, Name, Teams, Emails, DnList, AccessToken, RefreshToken, AccessExpirationTime, Groups) of
                       {ok, NewUser} ->
                           synchronize_spaces_info(NewUser, AccessToken);
                       {error, Error} ->
                           ?error("Sign in error: ~p", [Error]),
                           throw({0, Error})
                   end;
               Error ->
                   throw({1, Error})
           end,
    {get_login(User), User}.


%% create_user/6
%% ====================================================================
%% @doc
%% Creates a user in the DB.
%% @end
-spec create_user(GlobalId, Login, Name, Teams, Email, DnList) -> Result when
    GlobalId :: string(),
    Login :: string(),
    Name :: string(),
    Teams :: string(),
    Email :: [string()],
    DnList :: [string()],
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
create_user(GlobalId, Logins, Name, Teams, Email, DnList) ->
    create_user(GlobalId, Logins, Name, Teams, Email, DnList, <<>>, <<>>, 0).
create_user(GlobalId, Logins, Name, Teams, Email, DnList, AccessToken, RefreshToken, AccessExpirationTime) ->
  create_user(GlobalId, Logins, Name, Teams, Email, DnList, AccessToken, RefreshToken, AccessExpirationTime, []).
create_user(GlobalId, Logins, Name, Teams, Email, DnList, AccessToken, RefreshToken, AccessExpirationTime, Groups) ->
    ?debug("Creating user: ~p", [{GlobalId, Logins, Name, Teams, Email, DnList}]),
    Quota = #quota{},
    {QuotaAns, QuotaUUID} = dao_lib:apply(dao_users, save_quota, [Quota], 1),

    User = #user
    {
        global_id = GlobalId,
        logins = Logins,
        name = Name,
        teams = case Teams of
                    List when is_list(List) -> List;
                    _ -> []
                end,
        email_list = case Email of
                         List when is_list(List) -> List;
                         _ -> []
                     end,
        dn_list = case DnList of
                      List when is_list(List) -> List;
                      _ -> []
                  end,
        quota_doc = QuotaUUID,
        access_token = AccessToken,
        refresh_token = RefreshToken,
        access_expiration_time = AccessExpirationTime,
        groups = Groups
    },
    {DaoAns, UUID} = dao_lib:apply(dao_users, save_user, [#db_document{record = User, uuid = GlobalId}], 1),
    GetUserAns = get_user({uuid, UUID}),
    try
        case {DaoAns, QuotaAns} of
            {ok, ok} ->
                lists:foreach(
                    fun(#space_info{} = SP) ->
                        case fslogic_spaces:initialize(SP) of
                            {ok, _} -> ok;
                            {error, Reason} ->
                                throw(Reason)
                        end
                    end, get_spaces(User)),
                {ok, _} = GetUserAns;
            _ ->
                throw({error, {UUID, QuotaUUID}})
        end
    catch
        Type:Error ->
            ?error_stacktrace("Creating user failed with error: ~p", [Error]),
            case QuotaAns of
                ok ->
                    dao_lib:apply(dao_users, remove_quota, [QuotaUUID], 1);
                _ -> already_clean
            end,
            case DaoAns of
                ok ->
                    dao_lib:apply(dao_users, remove_user, [{uuid, UUID}], 1);
                _ -> already_clean
            end,
            {error, {Type, Error}}
    end.


%% get_user/1
%% ====================================================================
%% @doc
%% Retrieves user from DB by login, email, uuid, DN or rdnSequence proplist. Returns db_document wrapping a #user record.
%% @end
-spec get_user(Key :: user_key() |
{rdnSequence, [#'AttributeTypeAndValue'{}]}) ->
    {ok, user_doc()} | {error, any()}.
%% ====================================================================
get_user({rdnSequence, RDNSequence}) ->
    get_user({dn, rdn_sequence_to_dn_string(RDNSequence)});

get_user(Key) ->
    dao_lib:apply(dao_users, get_user, [Key], 1).


%% synchronize_spaces_info/2
%% ====================================================================
%% @doc Tries to synchronize spaces for given local user with globalregistry. This process involves downloading list of available to user spaces <br/>
%%      and initializing each of those spaces. This method never fails. On success, user's document is saved to DB.
%% @end
-spec synchronize_spaces_info(UserDoc :: #db_document{}, AccessToken :: binary()) -> UserDoc :: #db_document{}.
%% ====================================================================
synchronize_spaces_info(#db_document{record = #user{global_id = GlobalId} = UserRec} = UserDoc, AccessToken) ->
    case gr_users:get_spaces({user, AccessToken}) of
        {ok, #user_spaces{ids = SpaceIds, default = DefaultSpaceId}} ->
            ?info("Synchronized spaces for user ~p: ~p", [GlobalId, SpaceIds]),
            NewSpaces = case DefaultSpaceId of
                            <<"undefined">> ->
                                SpaceIds;
                            _ ->
                                [DefaultSpaceId | SpaceIds -- [DefaultSpaceId]]
                        end,

            ?debug("New spaces: ~p", [NewSpaces]),

            fslogic_context:set_gr_auth(GlobalId, AccessToken),
            [fslogic_spaces:initialize(SpaceId) || SpaceId <- NewSpaces],

            UserDoc1 = UserDoc#db_document{record = UserRec#user{spaces = NewSpaces}},
            case dao_lib:apply(dao_users, save_user, [UserDoc1], 1) of
                {ok, _} ->
                    UserDoc1;
                {error, Reason} ->
                    ?error("Cannot save user (while syncing spaces) due to: ~p", [Reason]),
                    UserDoc
            end;
        {error, Reason} ->
            ?error("Cannot synchronize user's (~p) spaces due to: ~p", [UserDoc#db_document.uuid, Reason]),
            UserDoc
    end.

%% synchronize_groups/2
%% ====================================================================
%% @doc Tries to synchronize groups for given local user with globalregistry.
%% This method never fails. On success, user's document is saved to DB.
%% @end
-spec synchronize_groups(UserDoc :: #db_document{}, AccessToken :: binary()) -> UserDoc :: #db_document{}.
%% ====================================================================
synchronize_groups(#db_document{record = #user{global_id = GlobalId} = UserRec} = UserDoc, AccessToken) ->
    case gr_users:get_groups({user, AccessToken}) of
        {ok, GroupIds} when is_list(GroupIds) ->
            ?debug("Synchronized groups for user ~p: ~p", [GlobalId, GroupIds]),
            UserDoc1 = UserDoc#db_document{record = UserRec#user{groups = GroupIds}},
            case dao_lib:apply(dao_users, save_user, [UserDoc1], 1) of
                {ok, _} ->
                    UserDoc1;
                {error, Reason} ->
                    ?error("Cannot save user (while syncing groups) due to: ~p", [Reason]),
                    UserDoc
            end;
        {error, Reason} ->
            ?error("Cannot synchronize user's (~p) groups due to: ~p", [UserDoc#db_document.uuid, Reason]),
            UserDoc
    end.

%% remove_user/1
%% ====================================================================
%% @doc
%% Removes user from DB by login.
%% @end
-spec remove_user(Key :: user_key() |
{rdnSequence, [#'AttributeTypeAndValue'{}]}) ->
    Result :: ok | {error, any()}.
%% ====================================================================
remove_user({rdnSequence, RDNSequence}) ->
    remove_user({dn, rdn_sequence_to_dn_string(RDNSequence)});

remove_user(Key) ->
    ?debug("Removing user: ~p", [Key]),
    GetUserAns = get_user(Key),
    {GetUserFirstAns, UserRec} = GetUserAns,
    case GetUserFirstAns of
        ok ->
            UserRec2 = UserRec#db_document.record,
            dao_lib:apply(dao_users, remove_quota, [UserRec2#user.quota_doc], 1);
        _ -> error
    end,
    dao_lib:apply(dao_users, remove_user, [Key], 1).


%% list_all_users/0
%% ====================================================================
%% @doc Lists all users
%% @end
-spec list_all_users() ->
    {ok, DocList :: list(#db_document{record :: #user{}})} |
    {error, atom()}.
%% ====================================================================
list_all_users() ->
    list_all_users(?DAO_LIST_BURST_SIZE, 0, []).

%% list_all_users/3
%% ====================================================================
%% @doc Returns given Actual list, concatenated with all users beginning
%%  from Offset (they will be get from dao in packages of size N)
%% @end
-spec list_all_users(N :: pos_integer(), Offset :: non_neg_integer(), Actual :: list(#db_document{record :: #user{}})) ->
    {ok, DocList :: list(#db_document{record :: #user{}})} |
    {error, atom()}.
%% ====================================================================
list_all_users(N, Offset, Actual) ->
    case dao_lib:apply(dao_users, list_users, [N, Offset], 1) of
        {ok, UserList} when length(UserList) == N ->
            list_all_users(N, Offset + N, Actual ++ UserList);
        {ok, FinalUserList} ->
            {ok, Actual ++ FinalUserList};
        {error, Error} ->
            {error, Error}
    end.


%% get_login/1
%% ====================================================================
%% @doc
%% Convinience function to get user login from #db_document encapsulating #user record.
%% @end
-spec get_login(User) -> Result when
    User :: user_doc(),
    Result :: string() | non_neg_integer().
%% ====================================================================
get_login(UserDoc) ->
    {{_, Login}, _} = get_login_with_uid(UserDoc),
    utils:ensure_list(Login).


%% get_login_with_uid/1
%% ====================================================================
%% @doc Returns username and storage uid of the user.
%% @end
-spec get_login_with_uid(User :: user_doc()) -> {{OpenidProvider :: atom(), UserName :: binary()}, SUID :: integer()}.
%% ====================================================================
get_login_with_uid(#db_document{uuid = ?CLUSTER_USER_ID}) ->
    {{internal, <<"root">>}, 0};
get_login_with_uid(#db_document{uuid = "0"}) ->
    {{internal, <<"root">>}, 0};
get_login_with_uid(#db_document{record = #user{logins = Logins0}, uuid = VCUID}) ->
    Logins = [Login || #id_token_login{login = LoginName, provider_id = Provider} = Login <- Logins0, size(LoginName) > 0, is_trusted_openid_provider(Provider)],

    StorageNameToUID =
        fun(Name) ->
            case ets:lookup(?STORAGE_USER_IDS_CACHE, unicode:characters_to_binary(Name)) of
                [{_, UID}] when UID >= 500 ->
                    UID;
                _ -> -1
            end
        end,

    LoginNamesWithUID = lists:map(
        fun(#id_token_login{login = LoginName, provider_id = ProviderId}) ->
            {{ProviderId, LoginName}, StorageNameToUID(LoginName)}
        end, Logins),

    LoginNamesWithUID1 = [{LoginName, UID} || {LoginName, UID} <- LoginNamesWithUID, UID >= 500],

    case LoginNamesWithUID1 of
        [] -> {{internal, <<"Unknown_", (utils:ensure_binary(VCUID))/binary>>}, fslogic_utils:gen_storage_uid(VCUID)};
        [{{ProviderId, LoginName}, UID} | _] ->
            {{ProviderId, utils:ensure_binary(LoginName)}, UID}
    end.


%% is_trusted_openid_provider/1
%% ====================================================================
%% @doc Checks if given openid provider is trusted by this provider (i.e. OpenID login can be used as OS username).
%% @end
-spec is_trusted_openid_provider(ProviderId :: atom()) -> boolean().
%% ====================================================================
is_trusted_openid_provider(internal) ->
    true;
is_trusted_openid_provider(Provider) when is_atom(Provider) ->
    {ok, Trusted} = oneprovider_node_app:get_env(trusted_openid_providers),
    lists:member(Provider, Trusted).


%% get_name/1
%% ====================================================================
%% @doc
%% Convinience function to get user name from #db_document encapsulating #user record.
%% @end
-spec get_name(User) -> Result when
    User :: user_doc(),
    Result :: string().
%% ====================================================================
get_name(User) ->
    User#db_document.record#user.name.


%% get_teams/1
%% ====================================================================
%% @doc
%% Convinience function to get user teams from #db_document encapsulating #user record.
%% @end
-spec get_teams(User) -> Result when
    User :: user_doc(),
    Result :: string().
%% ====================================================================
get_teams(User) ->
    User#db_document.record#user.teams.


%% update_name/2
%% ====================================================================
%% @doc
%% Update #db_document encapsulating #user record with new name and save it to DB.
%% @end
-spec update_name(User, NewName) -> Result when
    User :: user_doc(),
    NewName :: string(),
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_name(#db_document{record = UserInfo} = UserDoc, NewName) ->
    NewDoc = UserDoc#db_document{record = UserInfo#user{name = NewName}},
    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, UUID} ->
            dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
        {error, Reason} ->
            ?error("Cannot update user ~p teams: ~p", [UserInfo, Reason]),
            {error, Reason}
    end.


%% update_teams/2
%% ====================================================================
%% @doc
%% Update #db_document encapsulating #user record with new teams and save it to DB.
%% @end
-spec update_teams(User, NewTeams) -> Result when
    User :: user_doc(),
    NewTeams :: string(),
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_teams(#db_document{record = UserInfo} = UserDoc, NewTeams) ->
    NewDoc = UserDoc#db_document{record = UserInfo#user{teams = NewTeams}},
    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, UUID} ->
            dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
        {error, Reason} ->
            ?error("Cannot update user ~p teams: ~p", [UserInfo, Reason]),
            {error, Reason}
    end.

%% get_email_list/1
%% ====================================================================
%% @doc
%% Convinience function to get e-mail list from #db_document encapsulating #user record.
%% @end
-spec get_email_list(User) -> Result when
    User :: user_doc(),
    Result :: string().
%% ====================================================================
get_email_list(User) ->
    User#db_document.record#user.email_list.


%% update_email_list/2
%% ====================================================================
%% @doc
%% Update #db_document encapsulating #user record with new e-mail list and save it to DB.
%% @end
-spec update_email_list(User, NewEmailList) -> Result when
    User :: user_doc(),
    NewEmailList :: [string()],
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_email_list(#db_document{record = UserInfo} = UserDoc, NewEmailList) ->
    NewDoc = UserDoc#db_document{record = UserInfo#user{email_list = NewEmailList}},
    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, UUID} -> dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
        {error, Reason} ->
            ?error("Cannot update user ~p emails: ~p", [UserInfo, Reason]),
            {error, Reason}
    end.


%% get_dn_list/1
%% ====================================================================
%% @doc
%% Convinience function to get DN list from #db_document encapsulating #user record.
%% @end
-spec get_dn_list(User) -> Result when
    User :: user_doc(),
    Result :: string().
%% ====================================================================
get_dn_list(User) ->
    User#db_document.record#user.dn_list.


%% update_dn_list/2
%% ====================================================================
%% @doc
%% Update #db_document encapsulating #user record with new DN list and save it to DB.
%% @end
-spec update_dn_list(User, NewDnList) -> Result when
    User :: user_doc(),
    NewDnList :: [string()],
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_dn_list(#db_document{record = UserInfo} = UserDoc, NewDnList) ->
    NewDoc = UserDoc#db_document{record = UserInfo#user{dn_list = NewDnList}},
    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, UUID} -> dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
        {error, Reason} ->
            ?error("Cannot update user ~p dn_list: ~p", [UserInfo, Reason]),
            {error, Reason}
    end.


%% get_unverified_dn_list/1
%% ====================================================================
%% @doc
%% Convinience function to get unverified DN list from #db_document encapsulating #user record.
%% @end
-spec get_unverified_dn_list(User) -> Result when
    User :: user_doc(),
    Result :: string().
%% ====================================================================
get_unverified_dn_list(User) ->
    User#db_document.record#user.unverified_dn_list.


%% update_unverified_dn_list/2
%% ====================================================================
%% @doc
%% Update #db_document encapsulating #user record with new unverified DN list and save it to DB.
%% @end
-spec update_unverified_dn_list(User, NewDnList) -> Result when
    User :: user_doc(),
    NewDnList :: [string()],
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_unverified_dn_list(#db_document{record = UserInfo} = UserDoc, NewDnList) ->
    NewDoc = UserDoc#db_document{record = UserInfo#user{unverified_dn_list = NewDnList}},
    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, UUID} -> dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
        {error, Reason} -> {error, Reason}
    end.


%% get_role/1
%% ====================================================================
%% @doc
%% Convinience function to get user role from #db_document encapsulating #user record.
%% @end
-spec get_role(User) -> Result when
    User :: user_doc(),
    Result :: atom().
%% ====================================================================
get_role(User) ->
    User#db_document.record#user.role.


%% update_role/2
%% ====================================================================
%% @doc
%% Update #db_document encapsulating #user record with user role and save it to DB.
%% First argument can also be a key to get user from DB, as in get_user/1.
%% @end
-spec update_role(User, NewRole) -> Result when
    User :: user_doc() |
    {login, Login :: string()} |
    {email, Email :: string()} |
    {uuid, UUID :: user()} |
    {dn, DN :: string()} |
    {rdnSequence, [#'AttributeTypeAndValue'{}]},
    NewRole :: atom(),
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_role(#db_document{record = UserInfo} = UserDoc, NewRole) ->
    NewDoc = UserDoc#db_document{record = UserInfo#user{role = NewRole}},
    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, UUID} -> dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
        {error, Reason} ->
            ?error("Cannot update user ~p role: ~p", [UserInfo, Reason]),
            {error, Reason}
    end;

update_role(Key, NewRole) ->
    case get_user(Key) of
        {ok, UserDoc} ->
            #db_document{record = UserInfo} = UserDoc,
            NewDoc = UserDoc#db_document{record = UserInfo#user{role = NewRole}},
            case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
                {ok, UUID} -> dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
                {error, Reason} ->
                    ?error("Cannot update user ~p role: ~p", [Key, Reason]),
                    {error, Reason}
            end;
        Other -> Other
    end.

%% get_quota/1
%% ====================================================================
%% @doc
%% Convinience function to get #quota from #db_document encapsulating #user record.
%% @end
-spec get_quota(User) -> Result when
    User :: user_doc(),
    Result :: {ok, quota_info()} | {error, any()}.
%% ====================================================================
get_quota(User) ->
    case dao_lib:apply(dao_users, get_quota, [User#db_document.record#user.quota_doc], 1) of
        {ok, QuotaDoc} -> {ok, QuotaDoc#db_document.record};
        Other -> Other
    end.

%% update_quota/2
%% ====================================================================
%% @doc
%% Update #db_document encapsulating #quota record with new record and save it to DB.
%% @end
-spec update_quota(User, NewQuota) -> Result when
    User :: user_doc(),
    NewQuota :: quota_info(),
    Result :: {ok, quota()} | {error, any()}.
%% ====================================================================
update_quota(User, NewQuota) ->
    Quota = dao_lib:apply(dao_users, get_quota, [User#db_document.record#user.quota_doc], 1),
    case Quota of
        {ok, QuotaDoc} ->
            NewQuotaDoc = QuotaDoc#db_document{record = NewQuota},
            dao_lib:apply(dao_users, save_quota, [NewQuotaDoc], 1);
        {error, Error} ->
            ?error("Cannot update user ~p quota: ~p", [User, {get_quota_error, Error}]),
            {error, {get_quota_error, Error}};
        Other ->
            Other
    end.

%% get_files_size/2
%% ====================================================================
%% @doc Returns size of users' files
%% @end
-spec get_files_size(UUID :: uuid(), ProtocolVersion :: integer()) -> Result when
    Result :: {ok, Sum} | {error, any()},
    Sum :: non_neg_integer().
%% ====================================================================
get_files_size(UUID, ProtocolVersion) ->
    Ans = dao_lib:apply(dao_users, get_files_size, [UUID], ProtocolVersion),
    case Ans of
        {error, files_size_not_found} -> {ok, 0};
        _ -> Ans
    end.

%% rdn_sequence_to_dn_string/1
%% ====================================================================
%% @doc Converts rdnSequence to DN string so that it can be compared to another DN.
%% @end
-spec rdn_sequence_to_dn_string([#'AttributeTypeAndValue'{}]) -> {ok, string()} | {error, conversion_failed}.
%% ====================================================================
rdn_sequence_to_dn_string(RDNSequence) ->
    try
        DNString = lists:foldl(
            fun([#'AttributeTypeAndValue'{type = Type, value = Value}], Acc) ->
                ValueString = case Value of
                                  List when is_list(List) -> List;
                                  {_, List2} when is_list(List2) -> List2;
                                  {_, Binary} when is_binary(Binary) -> binary_to_list(Binary);
                                  _ -> throw({cannot_retrieve_value, Type, Value})
                              end,
                Acc ++ oid_code_to_shortname(Type) ++ "=" ++ ValueString ++ ","
            end, "", lists:reverse(RDNSequence)),

        %Remove tailing comma
        [_Comma | ProperString] = lists:reverse(DNString),
        {ok, lists:reverse(ProperString)}
    catch Type:Message ->
        ?error_stacktrace("Failed to convert rdnSequence to DN string.~n~p: ~p", [Type, Message]),
        {error, conversion_failed}
    end.


%% extract_dn_from_cert/1
%% ====================================================================
%% @doc Processes a .pem certificate and extracts subject (DN) part, returning it as an rdnSequence.
%% Returns an error if:
%%   - fails to extract DN
%%   - certificate is a proxy certificate -> {error, proxy_ceertificate}
%%   - certificate is self-signed -> {error, self_signed}
%% @end
-spec extract_dn_from_cert(PemBin :: binary()) -> {rdnSequence, [#'AttributeTypeAndValue'{}]} | {error, Reason} when
    Reason :: proxy_ceertificate | self_signed | extraction_failed.
%% ====================================================================
extract_dn_from_cert(PemBin) ->
    try
        Cert = public_key:pem_decode(PemBin),
        {_, DerCert, _} = lists:keyfind('Certificate', 1, Cert),
        OtpCert = public_key:pkix_decode_cert(DerCert, otp),

        IsProxy = gsi_handler:is_proxy_certificate(OtpCert),
        IsSelfSigned = public_key:pkix_is_self_signed(OtpCert),
        if
            IsProxy ->
                {error, proxy_certificate};
            IsSelfSigned ->
                {error, self_signed};
            true ->
                gsi_handler:proxy_subject(OtpCert)
        end
    catch _:_ ->
        {error, extraction_failed}
    end.


%% invert_dn_string/1
%% ====================================================================
%% @doc Inverts the sequence of entries in a DN string.
%% @end
-spec invert_dn_string(string()) -> string().
%% ====================================================================
invert_dn_string(DNString) ->
    Entries = string:tokens(DNString, ","),
    string:join(lists:reverse(Entries), ",").


%% update_access_credentials/4
%% ====================================================================
%% @doc
%% Update #db_document encapsulating #user record with new access token,
%% refresh token and access expiration time.
%% @end
-spec update_access_credentials(User, AccessToken :: binary(), RefreshToken :: binary(),
    ExpirationTime :: non_neg_integer()) -> Result when
    User :: user_doc(),
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_access_credentials(UserDoc, AccessToken, RefreshToken, ExpirationTime) ->
    NewDoc = update_access_credentials_int(UserDoc, AccessToken, RefreshToken, ExpirationTime),

    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, _} -> ok;
        {error, Reason} ->
            ?error("Cannot update user ~p access credentials: ~p", [UserDoc#db_document.uuid, Reason]),
            {error, Reason}
    end.
update_access_credentials_int(UserDoc, AccessToken, RefreshToken, ExpirationTime) ->
    #db_document{record = #user{global_id = GlobalId} = User} = UserDoc,
    fslogic_context:set_gr_auth(GlobalId, AccessToken),
    UserDoc#db_document{record = User#user{
        access_token = AccessToken,
        refresh_token = RefreshToken,
        access_expiration_time = ExpirationTime}}.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% synchronize_user_info/7
%% ====================================================================
%% @doc
%% This function synchronizes data from database with the information
%% received from OpenID provider.
%% @end
-spec synchronize_user_info(User, Logins, Teams, Emails, DnList, AccessToken :: binary(),
    RefreshToken :: binary(), AccessExpiration :: non_neg_integer()) -> Result when
    User :: user_doc(),
    Logins :: [#id_token_login{}],
    Teams :: string(),
    Emails :: [string()],
    DnList :: [string()],
    Result :: user_doc().
%% ====================================================================
synchronize_user_info(User, Logins, Teams, Emails, DnList, AccessToken, RefreshToken, AccessExpirationTime) ->
    %% Actual updates will probably happen so rarely, that there is no need to scoop those 3 DB updates into one.
    User1 = update_access_credentials_int(User, AccessToken, RefreshToken, AccessExpirationTime),
    User2 = case (get_teams(User1) =:= Teams) or (Teams =:= []) of
                true -> User1;
                false ->
                    {ok, NewUser} = update_teams(User1, Teams),
                    NewUser
            end,
    User3 =
        lists:foldl(fun(Email, UserDoc) ->
            case lists:member(Email, get_email_list(UserDoc)) or (Email =:= "") of
                true ->
                    UserDoc;
                false ->
                    {ok, NewUser2} = update_email_list(User2, get_email_list(User2) ++ [Email]),
                    NewUser2
            end
        end, User2, Emails),
    NewDns = lists:filter(
        fun(X) ->
            not lists:member(X, get_dn_list(User3))
        end, DnList),
    User4 = case NewDns of
                [] -> User3;
                _ ->
                    % Add DNs that are not yet on DN list
                    {ok, NewUser3} = update_dn_list(User3, get_dn_list(User3) ++ NewDns),
                    % And remove them from unverified list - if a DN comes from OpenID, it is as good as verified
                    {ok, NewUser4} = update_unverified_dn_list(NewUser3, get_unverified_dn_list(NewUser3) -- NewDns),
                    NewUser4
            end,

    User5 = User4#db_document{record = User4#db_document.record#user{logins = Logins}},

    User5.


%% oid_code_to_shortname/1
%% ====================================================================
%% @doc Converts erlang-like OID code to OpenSSL short name.
%% @end
-spec oid_code_to_shortname(term()) -> string() | no_return().
%% ====================================================================
oid_code_to_shortname(Code) ->
    try
        {Code, Shortname} = lists:keyfind(Code, 1, ?oid_code_to_shortname_mapping),
        Shortname
    catch _:_ ->
        throw({unknown_oid_code, Code})
    end.


%% shortname_to_oid_code/1
%% ====================================================================
%% @doc Converts OpenSSL short name to erlang-like OID code.
%% @end
-spec shortname_to_oid_code(string()) -> term() | no_return().
%% ====================================================================
shortname_to_oid_code(Shortname) ->
    try
        {Code, Shortname} = lists:keyfind(Shortname, 2, ?oid_code_to_shortname_mapping),
        Code
    catch _:_ ->
        throw({unknown_shortname, Shortname})
    end.

%% create_dirs_at_storage/1
%% ====================================================================
%% @doc Creates root dir for user and for its teams, on all storages
%% @end
-spec create_dirs_at_storage(SpacesInfo :: [#space_info{}]) -> ok | {error, Error} when
    Error :: atom().
%% ====================================================================
create_dirs_at_storage(SpacesInfo) ->
    {ListStatus, StorageList} = dao_lib:apply(dao_vfs, list_storage, [], 1),
    ?info("Creating dirs on storage for ~p / ~p", [SpacesInfo, StorageList]),
    case ListStatus of
        ok ->
            StorageRecords = lists:map(fun(DbDoc) -> DbDoc#db_document.record end, StorageList),
            CreateDirs = fun(StorageRecord, TmpAns) ->
                case create_dirs_at_storage(SpacesInfo, StorageRecord) of
                    ok -> TmpAns;
                    Error ->
                        ?error("Cannot create dirs ~p at storage, error: ~p", [SpacesInfo, Error]),
                        Error
                end
            end,
            lists:foldl(CreateDirs, ok, StorageRecords);
        Error2 ->
            ?error("Cannot create dirs ~p at storage, error: ~p", [SpacesInfo, {storage_listing_error, Error2}]),
            {error, storage_listing_error}
    end.

%% create_dirs_at_storage/2
%% ====================================================================
%% @doc Creates root dir for user's spaces. Only on selected storage
%% @end
-spec create_dirs_at_storage(SpacesInfo :: [#space_info{}], Storage :: #storage_info{}) -> ok | {error, Error} when
    Error :: atom().
%% ====================================================================
create_dirs_at_storage(SpacesInfo, Storage) ->
    SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),

    UserCtx = fslogic_context:get_user_context(),

    fslogic_context:clear_user_ctx(),

    CreateTeamsDirs = fun(#space_info{name = _SpaceName} = SpaceInfo, TmpAns) ->
        Dir = fslogic_spaces:get_storage_space_name(SpaceInfo),
        DirName = filename:join(["/", ?SPACES_BASE_DIR_NAME, Dir]),
        _Result = storage_files_manager:mkdir(SHI, filename:join(["/", ?SPACES_BASE_DIR_NAME]), ?EX_ALL_PERM bor ?RWE_USR_PERM),
        Ans = storage_files_manager:mkdir(SHI, DirName),
        case Ans of
            SuccessAns when SuccessAns == ok orelse SuccessAns == {error, dir_or_file_exists} ->
                Ans2 = storage_files_manager:chown(SHI, DirName, -1, fslogic_spaces:map_to_grp_owner(SpaceInfo)),
                Ans3 = storage_files_manager:chmod(SHI, DirName, 8#1731),
                case {Ans2, Ans3} of
                    {ok, ok} ->
                        TmpAns;
                    Error1 ->
                        ?error("Can not change owner of dir ~p using storage helper ~p due to ~p. Make sure group '~p' is defined in the system.",
                            [Dir, SHI#storage_helper_info.name, Error1, fslogic_spaces:map_to_grp_owner(SpaceInfo)]),
                        {error, dir_chown_error}
                end;
            Error ->
                ?error("Can not create dir ~p using storage ~p due to ~p. Make sure group ~p is defined in the system.",
                    [DirName, Storage, Error, fslogic_spaces:map_to_grp_owner(SpaceInfo)]),
                {error, create_dir_error}
        end
    end,

    Result = lists:foldl(CreateTeamsDirs, ok, SpacesInfo),
    fslogic_context:set_user_context(UserCtx),

    Result.

%% get_space_names/1
%% ====================================================================
%% @doc Returns list of spaces' names for given user. UserQuery shall be either #user{} record
%%      or query compatible with user_logic:get_user/1.
%%      The method assumes that user exists therefore will fail with exception when it doesnt.
%% @end
-spec get_space_names(UserQuery :: term()) -> [string()] | no_return().
%% ====================================================================
get_space_names(#db_document{record = #user{} = User}) ->
    get_space_names(User);
get_space_names(#user{} = User) ->
    [unicode:characters_to_list(SpaceName) || #space_info{name = SpaceName} <- get_spaces(User)];
get_space_names(UserQuery) ->
    {ok, UserDoc} = user_logic:get_user(UserQuery),
    get_space_names(UserDoc).


%% get_space_ids/1
%% ====================================================================
%% @doc Returns list of space IDs (as list of binary()) for given user. UserQuery shall be either #user{} record
%%      or query compatible with user_logic:get_user/1.
%%      The method assumes that user exists therefore will fail with exception when it doesn't.
%% @end
-spec get_space_ids(UserQuery :: term()) -> [binary()] | no_return().
%% ====================================================================
get_space_ids(#db_document{record = #user{} = User}) ->
    get_space_ids(User);
get_space_ids(#user{spaces = Spaces}) ->
    [utils:ensure_binary(SpaceId) || SpaceId <- Spaces];
get_space_ids(UserQuery) ->
    {ok, UserDoc} = user_logic:get_user(UserQuery),
    get_space_ids(UserDoc).

%% get_spaces/1
%% ====================================================================
%% @doc Returns list of spaces (as list of #space_info{}) for given user. UserQuery shall be either #user{} record
%%      or query compatible with user_logic:get_user/1.
%%      The method assumes that user exists therefore will fail with exception when it doesnt.
%% @end
-spec get_spaces(UserQuery :: term()) -> [#space_info{}] | no_return().
%% ====================================================================
get_spaces(#db_document{record = #user{} = User}) ->
    get_spaces(User);
get_spaces(#user{spaces = Spaces}) ->
    [SpaceInfo || {ok, #space_info{} = SpaceInfo} <- lists:map(fun(SpaceId) ->
        fslogic_objects:get_space({uuid, SpaceId}) end, Spaces)];
get_spaces(UserQuery) ->
    {ok, UserDoc} = user_logic:get_user(UserQuery),
    get_spaces(UserDoc).


%% create_space_dir/1
%% ====================================================================
%% @doc Creates directory (in DB) for given #space_info{}. If base spaces dir (/spaces) doesnt exists, its created too.
%%      Method will fail with exception error only when base spaces dir cannot be reached nor created.
%%      Otherwise standard {ok, ...} and  {error, ...} tuples will be returned.
%% @end
-spec create_space_dir(SpaceInfo :: #space_info{}) -> {ok, UUID :: uuid()} | {error, Reason :: any()} | no_return().
%% ====================================================================
create_space_dir(#space_info{space_id = SpaceId, name = SpaceName} = SpaceInfo) ->
    CTime = utils:time(),

    SpaceDirName = unicode:characters_to_list(SpaceName),

    GroupsBase = case dao_lib:apply(dao_vfs, exist_file, [{uuid, ?SPACES_BASE_DIR_NAME}], 1) of
                     {ok, true} ->
                         case dao_lib:apply(dao_vfs, get_file, [{uuid, ?SPACES_BASE_DIR_NAME}], 1) of
                             {ok, #db_document{uuid = UUID}} ->
                                 UUID;
                             {error, Reason} ->
                                 ?error("Error while getting groups base dir: ~p", [Reason]),
                                 error({error, Reason})
                         end;
                     {ok, false} ->
                         GFile = #file{type = ?DIR_TYPE, name = ?SPACES_BASE_DIR_NAME, uid = "0", parent = "", perms = 8#755},
                         GFileDoc = fslogic_meta:update_meta_attr(GFile, times, {CTime, CTime, CTime}),
                         case dao_lib:apply(dao_vfs, save_file, [#db_document{record = GFileDoc, uuid = ?SPACES_BASE_DIR_NAME}], 1) of
                             {ok, UUID} -> UUID;
                             {error, Reason} ->
                                 ?error("Error while creating groups base dir: ~p", [Reason]),
                                 error({error, Reason})
                         end;
                     {error, Reason} ->
                         ?error("Error while checking existence of groups base dir: ~p", [Reason]),
                         error({error, Reason})
                 end,

    case dao_lib:apply(dao_vfs, exist_file, [{uuid, SpaceId}], 1) of
        {ok, true} ->
            {error, dir_exists};
        {ok, false} ->
            TFile = #file{parent = GroupsBase, type = ?DIR_TYPE, name = SpaceDirName, uid = "0", perms = ?SpaceDirPerm, extensions = [{?file_space_info_extestion, SpaceInfo}]},
            TFileDoc = fslogic_meta:update_meta_attr(TFile, times, {CTime, CTime, CTime}),
            dao_lib:apply(dao_vfs, save_file, [#db_document{uuid = SpaceId, record = TFileDoc}], 1);
        Error ->
            Error
    end.

%% quota_exceeded/2
%% ====================================================================
%% @doc
%% Return true if quota is exceeded for user. Saves result to quota doc related to user. Throws an exception when user does not exists.
%% @end
-spec quota_exceeded(Key :: {login, Login :: string()} |
{email, Email :: string()} |
{uuid, UUID :: user()} |
{dn, DN :: string()} |
{rdnSequence, [#'AttributeTypeAndValue'{}]}, ProtocolVersion :: integer()) ->
    {boolean() | no_return()}.
%% ====================================================================
quota_exceeded(#db_document{uuid = Uuid, record = #user{}} = UserDoc, ProtocolVersion) ->
    {ok, SpaceUsed} = user_logic:get_files_size(Uuid, ProtocolVersion),
    {ok, #quota{size = Quota}} = user_logic:get_quota(UserDoc),
    ?info("user has used: ~p, quota: ~p", [SpaceUsed, Quota]),

    %% TODO: there is one little problem with this - we dont recognize situation in which quota has been manually changed to
    %% exactly the same value as default_quota
    {ok, DefaultQuotaSize} = application:get_env(?APP_Name, default_quota),
    QuotaToInsert = case Quota =:= DefaultQuotaSize of
                        true -> ?DEFAULT_QUOTA_DB_TAG;
                        _ -> Quota
                    end,

    case SpaceUsed > Quota of
        true ->
            update_quota(UserDoc, #quota{size = QuotaToInsert, exceeded = true}),
            true;
        _ ->
            update_quota(UserDoc, #quota{size = QuotaToInsert, exceeded = false}),
            false
    end;

quota_exceeded(UserQuery, ProtocolVersion) ->
    case user_logic:get_user(UserQuery) of
        {ok, UserDoc} ->
            quota_exceeded(UserDoc, ProtocolVersion);
        Error ->
            throw({cannot_fetch_user, Error})
    end.
