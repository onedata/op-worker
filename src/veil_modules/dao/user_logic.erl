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

-include("veil_modules/fslogic/fslogic.hrl").
-include_lib("ctool/include/logging.hrl").
-include("registered_names.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").


%% ====================================================================
%% API
%% ====================================================================
-export([sign_in/1, create_user/5, get_user/1, remove_user/1, list_all_users/0]).
-export([get_login/1, get_name/1, get_teams/1, update_teams/2]).
-export([get_email_list/1, update_email_list/2, get_role/1, update_role/2]).
-export([get_dn_list/1, update_dn_list/2, get_unverified_dn_list/1, update_unverified_dn_list/2]).
-export([rdn_sequence_to_dn_string/1, extract_dn_from_cert/1, invert_dn_string/1]).
-export([shortname_to_oid_code/1, oid_code_to_shortname/1]).
-export([get_space_names/1]).
-export([create_dirs_at_storage/3]).
-export([get_quota/1, update_quota/2, get_files_size/2, quota_exceeded/2]).

-define(UserRootPerms, 8#600).
-define(TeamDirPerm, 8#1770).

%% ====================================================================
%% Test API
%% ====================================================================
-ifdef(TEST).
-export([create_dirs_at_storage/2]).
-endif.

%% ====================================================================
%% API functions
%% ====================================================================

%% sign_in/1
%% ====================================================================
%% @doc
%% This function should be called after a user has logged in via OpenID. 
%% It looks the user up in database by login. If he is not there, it creates a proper document.
%% If the user already exists, synchronization is made between document
%% in the database and info received from OpenID provider.
%% @end
-spec sign_in(Proplist) -> Result when
    Proplist :: list(),
    Result :: {string(), user_doc()}.
%% ====================================================================
sign_in(Proplist) ->
    Login = proplists:get_value(login, Proplist, ""),
    Name = proplists:get_value(name, Proplist, ""),
    Teams = proplists:get_value(teams, Proplist, []),
    Email = proplists:get_value(email, Proplist, ""),
    DnList = proplists:get_value(dn_list, Proplist, []),

    User = case get_user({login, Login}) of
               {ok, ExistingUser} ->
                   synchronize_user_info(ExistingUser, Teams, Email, DnList);
               {error, user_not_found} ->
                    case create_user(Login, Name, Teams, Email, DnList) of
						{ok, NewUser} ->
							NewUser;
	                    {error,Error} ->
                        ?error("Sign in error: ~p", [Error]),
							throw(Error)
	               end;
    %% Code below is only for development purposes (connection to DB is not required)
               Error ->
                   throw(Error)
           end,
    {Login, User}.


%% create_user/5
%% ====================================================================
%% @doc
%% Creates a user in the DB.
%% @end
-spec create_user(Login, Name, Teams, Email, DnList) -> Result when
    Login :: string(),
    Name :: string(),
    Teams :: string(),
    Email :: string(),
    DnList :: [string()],
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
create_user(Login, Name, Teams, Email, DnList) ->
  ?debug("Creating user: ~p", [{Login, Name, Teams, Email, DnList}]),
	Quota = #quota{},
    {QuotaAns, QuotaUUID} = dao_lib:apply(dao_users, save_quota, [Quota], 1),
    User = #user
    {
        login = Login,
        name = Name,
%%         teams = case Teams of
%%                     Teams when is_list(Teams) -> Teams;
%%                     _ -> []
%%                 end,
        email_list = case Email of
                         Email when is_list(Email) -> [Email];
                         _ -> []
                     end,

        % Add login as first DN that user has - this enables use of GUI without having any
        % certificates registered. It bases on assumption that every login is unique.
        dn_list = case DnList of
                      List when is_list(List) -> [Login | List];
                      _ -> [Login]
                  end,
        quota_doc = QuotaUUID
    },
    {DaoAns, UUID} = dao_lib:apply(dao_users, save_user, [User], 1),
    try
        case {DaoAns, QuotaAns} of
            {ok, ok} ->
                GetUserAns = get_user({uuid, UUID}),

                [create_space_dir(Space) || Space <- get_space_names(User)], %% Create space dirs in DB if they don't exist

                {GetUserFirstAns, UserRec} = GetUserAns,
                case GetUserFirstAns of
                    ok ->
                        case create_dirs_at_storage(Login, get_spaces(User)) of
                            ok -> ok;
                            DirsError -> throw(DirsError)
                        end,
                        GetUserAns;
                    _ ->
                        throw(GetUserAns)
                end;
            _ ->
                throw({error, {UUID, QuotaUUID}})
        end
    catch
        _Type:Error ->
            ?error("Creating user failed with error: ~p", [Error]),
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
            Error
    end.


%% get_user/1
%% ====================================================================
%% @doc
%% Retrieves user from DB by login, email, uuid, DN or rdnSequence proplist. Returns veil_document wrapping a #user record.
%% @end
-spec get_user(Key :: user_key() |
{rdnSequence, [#'AttributeTypeAndValue'{}]}) ->
    {ok, user_doc()} | {error, any()}.
%% ====================================================================
get_user({rdnSequence, RDNSequence}) ->
    get_user({dn, rdn_sequence_to_dn_string(RDNSequence)});

get_user(Key) ->
    dao_lib:apply(dao_users, get_user, [Key], 1).


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
            UserRec2 = UserRec#veil_document.record,
            dao_lib:apply(dao_vfs, remove_file, [UserRec2#user.login], 1),
            dao_lib:apply(dao_users, remove_quota, [UserRec2#user.quota_doc], 1);
        _ -> error
    end,
    dao_lib:apply(dao_users, remove_user, [Key], 1).


%% list_all_users/0
%% ====================================================================
%% @doc Lists all users
%% @end
-spec list_all_users() ->
    {ok, DocList :: list(#veil_document{record :: #user{}})} |
    {error, atom()}.
%% ====================================================================
list_all_users() ->
    list_all_users(?DAO_LIST_BURST_SIZE, 0, []).

%% list_all_users/3
%% ====================================================================
%% @doc Returns given Actual list, concatenated with all users beginning
%%  from Offset (they will be get from dao in packages of size N)
%% @end
-spec list_all_users(N :: pos_integer(), Offset :: non_neg_integer(), Actual :: list(#veil_document{record :: #user{}})) ->
    {ok, DocList :: list(#veil_document{record :: #user{}})} |
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
%% Convinience function to get user login from #veil_document encapsulating #user record.
%% @end
-spec get_login(User) -> Result when
    User :: user_doc(),
    Result :: string().
%% ====================================================================
get_login(User) ->
    User#veil_document.record#user.login.


%% get_name/1
%% ====================================================================
%% @doc
%% Convinience function to get user name from #veil_document encapsulating #user record.
%% @end
-spec get_name(User) -> Result when
    User :: user_doc(),
    Result :: string().
%% ====================================================================
get_name(User) ->
    User#veil_document.record#user.name.


%% get_teams/1
%% ====================================================================
%% @doc
%% Convinience function to get user teams from #veil_document encapsulating #user record.
%% @end
-spec get_teams(User) -> Result when
    User :: user_doc(),
    Result :: string().
%% ====================================================================
get_teams(User) ->
    User#veil_document.record#user.teams.


%% update_teams/2
%% ====================================================================
%% @doc
%% Update #veil_document encapsulating #user record with new teams and save it to DB.
%% @end
-spec update_teams(User, NewTeams) -> Result when
    User :: user_doc(),
    NewTeams :: string(),
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_teams(#veil_document{record = UserInfo} = UserDoc, NewTeams) ->
    NewDoc = UserDoc#veil_document{record = UserInfo#user{teams = NewTeams}},
    [create_space_dir(Team) || Team <- get_space_names(NewDoc)], %% Create team dirs in DB if they dont exist
    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, UUID} ->
            create_dirs_at_storage(non, get_space_names(NewDoc)),
            dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
        {error, Reason} ->
          ?error("Cannot update user ~p teams: ~p", [UserInfo, Reason]),
          {error, Reason}
    end.

%% get_email_list/1
%% ====================================================================
%% @doc
%% Convinience function to get e-mail list from #veil_document encapsulating #user record.
%% @end
-spec get_email_list(User) -> Result when
    User :: user_doc(),
    Result :: string().
%% ====================================================================
get_email_list(User) ->
    User#veil_document.record#user.email_list.


%% update_email_list/2
%% ====================================================================
%% @doc
%% Update #veil_document encapsulating #user record with new e-mail list and save it to DB.
%% @end
-spec update_email_list(User, NewEmailList) -> Result when
    User :: user_doc(),
    NewEmailList :: [string()],
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_email_list(#veil_document{record = UserInfo} = UserDoc, NewEmailList) ->
    NewDoc = UserDoc#veil_document{record = UserInfo#user{email_list = NewEmailList}},
    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, UUID} -> dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
        {error, Reason} ->
          ?error("Cannot update user ~p emails: ~p", [UserInfo, Reason]),
          {error, Reason}
    end.


%% get_dn_list/1
%% ====================================================================
%% @doc
%% Convinience function to get DN list from #veil_document encapsulating #user record.
%% @end
-spec get_dn_list(User) -> Result when
    User :: user_doc(),
    Result :: string().
%% ====================================================================
get_dn_list(User) ->
    User#veil_document.record#user.dn_list.


%% update_dn_list/2
%% ====================================================================
%% @doc
%% Update #veil_document encapsulating #user record with new DN list and save it to DB.
%% @end
-spec update_dn_list(User, NewDnList) -> Result when
    User :: user_doc(),
    NewDnList :: [string()],
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_dn_list(#veil_document{record = UserInfo} = UserDoc, NewDnList) ->
    NewDoc = UserDoc#veil_document{record = UserInfo#user{dn_list = NewDnList}},
    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, UUID} -> dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
        {error, Reason} ->
          ?error("Cannot update user ~p dn_list: ~p", [UserInfo, Reason]),
          {error, Reason}
    end.


%% get_unverified_dn_list/1
%% ====================================================================
%% @doc
%% Convinience function to get unverified DN list from #veil_document encapsulating #user record.
%% @end
-spec get_unverified_dn_list(User) -> Result when
    User :: user_doc(),
    Result :: string().
%% ====================================================================
get_unverified_dn_list(User) ->
    User#veil_document.record#user.unverified_dn_list.


%% update_unverified_dn_list/2
%% ====================================================================
%% @doc
%% Update #veil_document encapsulating #user record with new unverified DN list and save it to DB.
%% @end
-spec update_unverified_dn_list(User, NewDnList) -> Result when
    User :: user_doc(),
    NewDnList :: [string()],
    Result :: {ok, user_doc()} | {error, any()}.
%% ====================================================================
update_unverified_dn_list(#veil_document{record = UserInfo} = UserDoc, NewDnList) ->
    NewDoc = UserDoc#veil_document{record = UserInfo#user{unverified_dn_list = NewDnList}},
    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, UUID} -> dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
        {error, Reason} -> {error, Reason}
    end.


%% get_role/1
%% ====================================================================
%% @doc
%% Convinience function to get user role from #veil_document encapsulating #user record.
%% @end
-spec get_role(User) -> Result when
    User :: user_doc(),
    Result :: atom().
%% ====================================================================
get_role(User) ->
    User#veil_document.record#user.role.


%% update_role/2
%% ====================================================================
%% @doc
%% Update #veil_document encapsulating #user record with user role and save it to DB.
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
update_role(#veil_document{record = UserInfo} = UserDoc, NewRole) ->
    NewDoc = UserDoc#veil_document{record = UserInfo#user{role = NewRole}},
    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, UUID} -> dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
        {error, Reason} ->
          ?error("Cannot update user ~p role: ~p", [UserInfo, Reason]),
          {error, Reason}
    end;

update_role(Key, NewRole) ->
    case get_user(Key) of
        {ok, UserDoc} ->
            #veil_document{record = UserInfo} = UserDoc,
            NewDoc = UserDoc#veil_document{record = UserInfo#user{role = NewRole}},
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
%% Convinience function to get #quota from #veil_document encapsulating #user record.
%% @end
-spec get_quota(User) -> Result when
    User :: user_doc(),
    Result :: {ok, quota_info()} | {error, any()}.
%% ====================================================================
get_quota(User) ->
    case dao_lib:apply(dao_users, get_quota, [User#veil_document.record#user.quota_doc], 1) of
        {ok, QuotaDoc} -> {ok, QuotaDoc#veil_document.record};
        Other -> Other
    end.

%% update_quota/2
%% ====================================================================
%% @doc
%% Update #veil_document encapsulating #quota record with new record and save it to DB.
%% @end
-spec update_quota(User, NewQuota) -> Result when
    User :: user_doc(),
    NewQuota :: quota_info(),
    Result :: {ok, quota()} | {error, any()}.
%% ====================================================================
update_quota(User, NewQuota) ->
  Quota = dao_lib:apply(dao_users, get_quota, [User#veil_document.record#user.quota_doc], 1),
  case Quota of
    {ok, QuotaDoc} ->
      NewQuotaDoc = QuotaDoc#veil_document{record = NewQuota},
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


%% ====================================================================
%% Internal functions
%% ====================================================================

%% synchronize_user_info/4
%% ====================================================================
%% @doc
%% This function synchronizes data from database with the information
%% received from OpenID provider.
%% @end
-spec synchronize_user_info(User, Teams, Email, DnList) -> Result when
    User :: user_doc(),
    Teams :: string(),
    Email :: string(),
    DnList :: [string()],
    Result :: user_doc().
%% ====================================================================
synchronize_user_info(User, Teams, Email, DnList) ->
    %% Actual updates will probably happen so rarely, that there is no need to scoop those 3 DB updates into one.
    User2 = case (get_teams(User) =:= Teams) or (Teams =:= []) of
                true -> User;
                false ->
                    {ok, NewUser} = update_teams(User, Teams),
                    NewUser
            end,

    User3 = case lists:member(Email, get_email_list(User2)) or (Email =:= "") of
                true -> User2;
                false ->
                    {ok, NewUser2} = update_email_list(User2, get_email_list(User2) ++ [Email]),
                    NewUser2
            end,

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
    User4.


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

%% create_dirs_at_storage/2
%% ====================================================================
%% @doc Creates root dir for user and for its teams, on all storages
%% @end
-spec create_dirs_at_storage(Root :: string(), SpacesInfo :: [#space_info{}]) -> ok | {error, Error} when
    Error :: atom().
%% ====================================================================
create_dirs_at_storage(Root, SpacesInfo) ->
    {ListStatus, StorageList} = dao_lib:apply(dao_vfs, list_storage, [], 1),
    case ListStatus of
        ok ->
            StorageRecords = lists:map(fun(VeilDoc) -> VeilDoc#veil_document.record end, StorageList),
            CreateDirs = fun(StorageRecord,TmpAns) ->
                case create_dirs_at_storage(Root,SpacesInfo,StorageRecord) of
                    ok -> TmpAns;
                    Error ->
                      ?error("Cannot create dirs ~p at storage, error: ~p", [{Root, SpacesInfo}, Error]),
                      Error
                end
            end,
            lists:foldl(CreateDirs,ok , StorageRecords);
        Error2 ->
          ?error("Cannot create dirs ~p at storage, error: ~p", [{Root, SpacesInfo}, {storage_listing_error, Error2}]),
          {error,storage_listing_error}
    end.

%% create_dirs_at_storage/3
%% ====================================================================
%% @doc Creates root dir for user and for its teams. Only on selected storage
%% @end
-spec create_dirs_at_storage(Root :: string(), SpacesInfo :: [#space_info{}], Storage :: #storage_info{}) -> ok | {error, Error} when
    Error :: atom().
%% ====================================================================
create_dirs_at_storage(Root, SpacesInfo, Storage) ->
    SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),

    CreateTeamsDirs = fun(#space_info{name = Dir}, TmpAns) ->
        DirName = filename:join(["", ?SPACES_BASE_DIR_NAME, Dir]),
        Ans = storage_files_manager:mkdir(SHI, DirName),
        case Ans of
            SuccessAns when SuccessAns == ok orelse SuccessAns == {error, dir_or_file_exists} ->
                Ans2 = storage_files_manager:chown(SHI, DirName, "", fslogic_spaces:map_to_grp_owner(Dir)),
                Ans3 = storage_files_manager:chmod(SHI, DirName, 8#1750),
                case {Ans2, Ans3} of
                    {ok, ok} ->
                        TmpAns;
                    Error1 ->
                        ?error("Can not change owner of dir ~p using storage helper ~p due to ~p. Make sure group '~s' is defined in the system.",
                            [Dir, SHI#storage_helper_info.name, Error1, Dir]),
                        {error, dir_chown_error},
                        TmpAns
                end;
            _ ->
                ?error("Can not create dir ~p using storage helper ~p. Make sure group '~s' is defined in the system.",
                    [Dir, SHI#storage_helper_info.name, Dir]),
                {error, create_dir_error}
        end
    end,

    lists:foldl(CreateTeamsDirs, ok, SpacesInfo).

%% get_team_names/1
%% ====================================================================
%% @doc Returns list of group/team names for given user. UserQuery shall be either #user{} record
%%      or query compatible with user_logic:get_user/1.
%%      The method assumes that user exists therefore will fail with exception when it doesnt.
%% @end
-spec get_space_names(UserQuery :: term()) -> [string()] | no_return().
%% ====================================================================
get_space_names(#veil_document{record = #user{} = User}) ->
    get_space_names(User);
get_space_names(#user{} = User) ->
    [SpaceName || #space_info{name = SpaceName} <- get_spaces(User)];
get_space_names(UserQuery) ->
    {ok, UserDoc} = user_logic:get_user(UserQuery),
    get_space_names(UserDoc).

get_spaces(#veil_document{record = #user{} = User}) ->
    get_spaces(User);
get_spaces(#user{spaces = Spaces}) ->
    [SpaceInfo || {ok, #space_info{}} = SpaceInfo <- lists:map(fun(SpaceId) -> fslogic_objects:get_space(SpaceId) end, Spaces)];
get_spaces(UserQuery) ->
    {ok, UserDoc} = user_logic:get_user(UserQuery),
    get_spaces(UserDoc).


%% create_team_dir/1
%% ====================================================================
%% @doc Creates directory (in DB) for given group/team name. If base group dir (/groups) doesnt exists, its created too.
%%      Method will fail with exception error only when base group dir cannot be reached nor created.
%%      Otherwise standard {ok, ...} and  {error, ...} tuples will be returned.
%% @end
-spec create_space_dir(Dir :: string()) -> {ok, UUID :: uuid()} | {error, Reason :: any()} | no_return().
%% ====================================================================
create_space_dir(TeamName) ->
    CTime = vcn_utils:time(),
    GroupsBase = case dao_lib:apply(dao_vfs, exist_file, ["/" ++ ?SPACES_BASE_DIR_NAME], 1) of
                     {ok, true} ->
                         case dao_lib:apply(dao_vfs, get_file, ["/" ++ ?SPACES_BASE_DIR_NAME], 1) of
                             {ok, #veil_document{uuid = UUID}} ->
                                 UUID;
                             {error, Reason} ->
                                 ?error("Error while getting groups base dir: ~p", [Reason]),
                                 error({error, Reason})
                         end;
                     {ok, false} ->
                         GFile = #file{type = ?DIR_TYPE, name = ?SPACES_BASE_DIR_NAME, uid = "0", parent = "", perms = 8#555},
                         GFileDoc = fslogic_meta:update_meta_attr(GFile, times, {CTime, CTime, CTime}),
                         case dao_lib:apply(dao_vfs, save_new_file, ["/" ++ ?SPACES_BASE_DIR_NAME, GFileDoc], 1) of
                             {ok, UUID} -> UUID;
                             {error, Reason} ->
                                 ?error("Error while creating groups base dir: ~p", [Reason]),
                                 error({error, Reason})
                         end;
                     {error, Reason} ->
                         ?error("Error while checking existence of groups base dir: ~p", [Reason]),
                         error({error, Reason})
                 end,

    case dao_lib:apply(dao_vfs, exist_file, ["/" ++ ?SPACES_BASE_DIR_NAME ++ "/" ++ TeamName], 1) of
        {ok, true} ->
            {error, dir_exists};
        {ok, false} ->
            TFile = #file{type = ?DIR_TYPE, name = TeamName, uid = "0", gids = [TeamName], parent = GroupsBase, perms = ?TeamDirPerm},
            TFileDoc = fslogic_meta:update_meta_attr(TFile, times, {CTime, CTime, CTime}),
            dao_lib:apply(dao_vfs, save_new_file, ["/" ++ ?SPACES_BASE_DIR_NAME ++ "/" ++ TeamName, TFileDoc], 1);
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
quota_exceeded(UserQuery, ProtocolVersion) ->
    case user_logic:get_user(UserQuery) of
        {ok, UserDoc} ->
            Uuid = UserDoc#veil_document.uuid,
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
        Error ->
            throw({cannot_fetch_user, Error})
    end.
