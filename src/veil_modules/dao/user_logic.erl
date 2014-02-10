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
-include("logging.hrl").
-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/dao_types.hrl").


%% ====================================================================
%% API
%% ====================================================================
-export([sign_in/1, create_user/5, get_user/1, remove_user/1]).
-export([get_login/1, get_name/1, get_teams/1, update_teams/2]).
-export([get_email_list/1, update_email_list/2, get_dn_list/1, update_dn_list/2]).
-export([rdn_sequence_to_dn_string/1, extract_dn_from_cert/1, invert_dn_string/1]).
-export([shortname_to_oid_code/1, oid_code_to_shortname/1]).
-export([get_team_names/1]).

-define(UserRootPerms, 8#600).

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
                   {ok, NewUser} = create_user(Login, Name, Teams, Email, DnList),
                   NewUser;
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
    User = #user
    {
        login = Login,
        name = Name,
        teams = case Teams of
                    Teams when is_list(Teams) -> Teams;
                    _ -> []
                end,
        email_list = case Email of
                         Email when is_list(Email) -> [Email];
                         _ -> []
                     end,
        dn_list = case DnList of
                      List when is_list(List) -> List;
                      _ -> []
                  end
    },
    {ok, UUID} = dao_lib:apply(dao_users, save_user, [User], 1),
    GetUserAns = get_user({uuid, UUID}),

    [create_team_dir(Team) || Team <- get_team_names(User)], %% Create team dirs in DB if they dont exist

    {GetUserFirstAns, UserRec} = GetUserAns,
    case GetUserFirstAns of
        ok ->
            RootAns = create_root(Login, UserRec#veil_document.uuid),
            case RootAns of
                ok ->
                    %% TODO zastanowić się co zrobić jak nie uda się stworzyć jakiegoś katalogu (blokowanie rejestracji użytkownika to chyba zbyt dużo)
                    create_dirs_at_storage(Login, get_team_names(User)),
                    GetUserAns;
                _ -> {RootAns, UserRec}
            end;
        _ -> GetUserAns
    end.


%% get_user/1
%% ====================================================================
%% @doc
%% Retrieves user from DB by login, email, uuid, DN or rdnSequence proplist. Returns veil_document wrapping a #user record.
%% @end
-spec get_user(Key :: {login, Login :: string()} |
{email, Email :: string()} |
{uuid, UUID :: user()} |
{dn, DN :: string()} |
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
-spec remove_user(Key :: {login, Login :: string()} |
{email, Email :: string()} |
{uuid, UUID :: user()} |
{dn, DN :: string()} |
{rdnSequence, [#'AttributeTypeAndValue'{}]}) ->
    Result :: ok | {error, any()}.
%% ====================================================================
remove_user({rdnSequence, RDNSequence}) ->
    remove_user({dn, rdn_sequence_to_dn_string(RDNSequence)});

remove_user(Key) ->
    GetUserAns = get_user(Key),
    {GetUserFirstAns, UserRec} = GetUserAns,
    case GetUserFirstAns of
        ok ->
            UserRec2 = UserRec#veil_document.record,
            dao_lib:apply(dao_vfs, remove_file, [UserRec2#user.login], 1);
        _ -> error
    end,
    dao_lib:apply(dao_users, remove_user, [Key], 1).


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
    [create_team_dir(Team) || Team <- get_team_names(NewDoc)], %% Create team dirs in DB if they dont exist
    case dao_lib:apply(dao_users, save_user, [NewDoc], 1) of
        {ok, UUID} ->
            create_dirs_at_storage(non, get_team_names(NewDoc)),
            dao_lib:apply(dao_users, get_user, [{uuid, UUID}], 1);
        {error, Reason} -> {error, Reason}
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
        {error, Reason} -> {error, Reason}
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
        {error, Reason} -> {error, Reason}
    end.


%% rdn_sequence_to_dn_string/1
%% ====================================================================
%% @doc Converts rdnSequence to DN string so that it can be compared to another DN.
%% @end
-spec rdn_sequence_to_dn_string([#'AttributeTypeAndValue'{}]) -> string() | no_return().
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
        lager:error("Failed to convert rdnSequence to DN string.~n~p: ~p~n~p", [Type, Message, erlang:get_stacktrace()]),
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
                List ->
                    {ok, NewUser3} = update_dn_list(User3, get_dn_list(User3) ++ List),
                    NewUser3
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


%% create_root/2
%% ====================================================================
%% @doc Creates root directory for user.
%% @end
-spec create_root(Dir :: string(), Uid :: term()) -> ok | Error when
    Error :: atom().
%% ====================================================================
create_root(Dir, Uid) ->
    {ParentFound, ParentInfo} = fslogic_utils:get_parent_and_name_from_path(Dir, 1),
    case ParentFound of
        ok ->
            {FileName, Parent} = ParentInfo,
            File = #file{type = ?DIR_TYPE, name = FileName, uid = Uid, parent = Parent#veil_document.uuid, perms = ?UserRootPerms},
            CTime = fslogic_utils:time(),
            FileDoc = fslogic_utils:update_meta_attr(File, times, {CTime, CTime, CTime}),
            SaveAns = dao_lib:apply(dao_vfs, save_new_file, [Dir, FileDoc], 1),
            case SaveAns of
                {error, file_exists} ->
                    root_exists;
                _ -> ok
            end;
        _ParentError -> parent_error
    end.

%% create_dirs_at_storage/2
%% ====================================================================
%% @doc Creates root dir for user and for its teams
%% @end
-spec create_dirs_at_storage(Root :: string(), Teams :: [string()]) -> ok | Error when
    Error :: atom().
%% ====================================================================
create_dirs_at_storage(Root, Teams) ->
    {ListStatus, StorageList} = dao_lib:apply(dao_vfs, list_storage, [], 1),
    case ListStatus of
        ok ->
            CreateTeamsDirs = fun(Dir, {SHInfo, TmpAns}) ->
                DirName = "groups/" ++ Dir,
                Ans = storage_files_manager:mkdir(SHInfo, DirName),
                case Ans of
                    ok ->
                        Ans2 = storage_files_manager:chown(SHInfo, DirName, "", Dir),
                        Ans3 = storage_files_manager:chmod(SHInfo, DirName, 8#730),
                        case {Ans2, Ans3} of
                            {ok, ok} ->
                                {SHInfo, TmpAns};
                            _ ->
                                lager:error("Can not change owner of dir ~p using storage helper ~p. Make sure group '~s' is defined in the system.",
                                    [Dir, SHInfo#storage_helper_info.name, Dir]),
                                {SHInfo, error}
                        end;
                    _ ->
                        lager:error("Can not create dir ~p using storage helper ~p. Make sure group '~s' is defined in the system.",
                            [Dir, SHInfo#storage_helper_info.name, Dir]),
                        {SHInfo, error}
                end
            end,

            CreateDirsOnStorage = fun(Storage, TmpAns) ->
                SHI = fslogic_storage:get_sh_for_fuse(?CLUSTER_FUSE_ID, Storage),
                Ans2 = case Root of
                           non ->
                               ok;
                           _ ->
                               RootDirName = "users/" ++ Root,
                               Ans = storage_files_manager:mkdir(SHI, RootDirName),
                               case Ans of
                                   ok ->
                                       Ans3 = storage_files_manager:chown(SHI, RootDirName, Root, Root),
                                       storage_files_manager:chmod(SHI, RootDirName, 8#300),
                                       case Ans3 of
                                           ok ->
                                               ok;
                                           _ ->
                                               lager:error("Can not change owner of dir ~p using storage helper ~p. Make sure user '~s' is defined in the system.",
                                                   [Root, SHI#storage_helper_info.name, Root])
                                       end;
                                   _ ->
                                       lager:error("Can not create dir ~p using storage helper ~p. Make sure user '~s' is defined in the system.",
                                           [Root, SHI#storage_helper_info.name, Root])
                               end
                       end,

                Ans4 = lists:foldl(CreateTeamsDirs, {SHI, ok}, Teams),
                case {Ans2, Ans4} of
                    {ok, ok} -> TmpAns;
                    _ -> create_dir_error
                end
            end,

            lists:foldl(CreateDirsOnStorage, ok, lists:map(fun(VeilDoc) ->
                VeilDoc#veil_document.record end, StorageList));
        _ -> storage_listing_error
    end.


%% get_team_names/1
%% ====================================================================
%% @doc Returns list of group/team names for given user. UserQuery shall be either #user{} record
%%      or query compatible with user_logic:get_user/1.
%%      The method assumes that user exists therefore will fail with exception when it doesnt.
%% @end
-spec get_team_names(UserQuery :: term()) -> [string()] | no_return().
%% ====================================================================
get_team_names(#veil_document{record = #user{} = User}) ->
    get_team_names(User);
get_team_names(#user{} = User) ->
    [string:strip(lists:nth(1, string:tokens(X, "("))) || X <- User#user.teams];
get_team_names(UserQuery) ->
    {ok, UserDoc} = user_logic:get_user(UserQuery),
    get_team_names(UserDoc).


%% create_team_dir/1
%% ====================================================================
%% @doc Creates directory (in DB) for given group/team name. If base group dir (/groups) doesnt exists, its created too.
%%      Method will fail with exception error only when base group dir cannot be reached nor created.
%%      Otherwise standard {ok, ...} and  {error, ...} tuples will be returned.
%% @end
-spec create_team_dir(Dir :: string()) -> {ok, UUID :: uuid()} | {error, Reason :: any()} | no_return().
%% ====================================================================
create_team_dir(TeamName) ->
    CTime = fslogic_utils:time(),
    GFile = #file{type = ?DIR_TYPE, name = ?GROUPS_BASE_DIR_NAME, uid = "0", parent = "", perms = 8#555},
    GFileDoc = fslogic_utils:update_meta_attr(GFile, times, {CTime, CTime, CTime}),
    {SaveStatus, UUID} = dao_lib:apply(dao_vfs, save_new_file, ["/" ++ ?GROUPS_BASE_DIR_NAME, GFileDoc], 1),
    GroupsBase = case {SaveStatus, UUID} of
                     {ok, _} -> UUID;
                     {error, file_exists} ->
                         case dao_lib:apply(dao_vfs, get_file, ["/" ++ ?GROUPS_BASE_DIR_NAME], 1) of
                             {ok, #veil_document{uuid = UUID1}} -> UUID1;
                             {error, Reason2} ->
                                 ?error("Error while getting groups base dir: ~p", [Reason2]),
                                 error({error, Reason2})
                         end;
                     {error, Reason} ->
                         ?error("Error while getting groups base dir: ~p", [Reason]),
                         error({error, Reason})
                 end,

    TFile = #file{type = ?DIR_TYPE, name = TeamName, uid = "0", gids = [TeamName], parent = GroupsBase, perms = 8#770},
    TFileDoc = fslogic_utils:update_meta_attr(TFile, times, {CTime, CTime, CTime}),
    case dao_lib:apply(dao_vfs, save_new_file, ["/" ++ ?GROUPS_BASE_DIR_NAME ++ "/" ++ TeamName, TFileDoc], 1) of
        {error, file_exists} -> {error, dir_exists};
        Other -> Other
    end.
