%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc check_permissions annotation implementation.
%%%      This annotation shall check whether annotation's caller has given
%%%      permissions to file that is also somewhere within annotation's arguments.
%%% @end
%%%-------------------------------------------------------------------
-module(check_permissions).
-annotation('function').
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([before_advice/4, after_advice/5, get_validation_subject/2]).

%% Object pointing to annotation's argument which holds file data (see resolve_file/2)
-type item_definition() :: non_neg_integer() | {path, non_neg_integer()} | {uuid, file_meta:uuid()} | {parent, item_definition()}.
-type check_type() :: owner % Check whether user owns the item
                    | traverse_ancestors % validates ancestors' exec permission.
                    | owner_if_parent_sticky % Check whether user owns the item but only if parent of the item has sticky bit.
                    | write
                    | read
                    | exec
                    | rdwr
                    | acl_access_mask().

-type acl_access_mask() :: binary().
-type access_definition() :: root | {check_type(), item_definition()}.

-export_type([check_type/0, item_definition/0, access_definition/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc annotation's before_advice implementation.
%%      #annotation.data type is [access_definition()] | access_definition()
%%--------------------------------------------------------------------
-spec before_advice(#annotation{data :: [access_definition()]}, module(), atom(), [term()]) -> term().
before_advice(#annotation{data = AccessDefinitions}, _M, _F,
  Args = [#fslogic_ctx{session = #session{identity = #identity{user_id = UserId}}} | _]) ->
    ExpandedAccessDefinitions = expand_access_definitions(AccessDefinitions, UserId, Args, #{}, #{}, #{}),
    [ok = rules:check(Def) || Def <- ExpandedAccessDefinitions],
    Args;
before_advice(#annotation{data = AccessDefinitions}, _M, _F, [#sfm_handle{session_id = SessionId, file_uuid = FileUUID} = Handle | RestOfArgs] = Args) ->
    {ok, #document{value = #session{identity = #identity{user_id = UserId}}}} = session:get(SessionId),
    ExpandedAccessDefinitions = expand_access_definitions(AccessDefinitions, UserId, Args, #{}, #{}, #{}),
    [ok = rules:check(Def) || Def <- ExpandedAccessDefinitions],
    case (catch has_acl(FileUUID)) of
        true ->
            [Handle#sfm_handle{session_id = ?ROOT_SESS_ID} | RestOfArgs];
        _ ->
            Args
    end.

%%--------------------------------------------------------------------
%% @doc annotation's after_advice implementation.
%%--------------------------------------------------------------------
-spec after_advice(#annotation{}, atom(), atom(), [term()], term()) -> term().
after_advice(#annotation{}, _M, _F, _Inputs, Result) ->
    Result.

%%--------------------------------------------------------------------
%% @doc
%% Expand access definition to form allowing it to be verified by rules module.
%% @end
%%--------------------------------------------------------------------
-spec expand_access_definitions([access_definition()], onedata_user:id(), list(), #{}, #{}, #{}) ->
    [{check_type(), datastore:document(), datastore:document(), [#accesscontrolentity{}]}].
expand_access_definitions([], _UserId, _Inputs, _FileMap, _AclMap, _UserMap) ->
    [];
expand_access_definitions(_, ?ROOT_USER_ID, _Inputs, _FileMap, _AclMap, _UserMap) ->
    [];
expand_access_definitions([root | Rest], UserId, Inputs, FileMap, AclMap, UserMap) ->
    {User, NewUserMap} = get_user(UserId, UserMap),
    [{root, undefined, User, undefined}  | expand_access_definitions(Rest, UserId, Inputs, FileMap, AclMap, NewUserMap)];
expand_access_definitions([{traverse_ancestors, ItemDefinition} | Rest], UserId, Inputs, FileMap, AclMap, UserMap) ->
    {User, NewUserMap} = get_user(UserId, UserMap),
    {ExpandedTraverseDefs, NewFileMap} = % if file is a scope check scope access also
        case (catch get_file(ItemDefinition, FileMap, UserId, Inputs)) of
            {#document{value = #file_meta{is_scope = true}} = File, NewFileMap} ->
                {expand_traverse_ancestors_check(File, User, AclMap), NewFileMap};
            _ ->
                {ParentFile, NewFileMap} = get_file({parent, ItemDefinition}, FileMap, UserId, Inputs),
                {expand_traverse_ancestors_check(ParentFile, User, AclMap), NewFileMap}
        end,

    ExpandedTraverseDefs
    ++ expand_access_definitions(Rest, UserId, Inputs, NewFileMap, AclMap, NewUserMap);
expand_access_definitions([{CheckType, ItemDefinition} | Rest], UserId, Inputs, FileMap, AclMap, UserMap) ->
    {File = #document{key = Key}, NewFileMap} = get_file(ItemDefinition, FileMap, UserId, Inputs),
    {Acl, NewAclMap} = get_acl(Key, AclMap),
    {User, NewUserMap} = get_user(UserId, UserMap),
    [{CheckType, File, User, Acl} | expand_access_definitions(Rest, UserId, Inputs, NewFileMap, NewAclMap, NewUserMap)].

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Returns file that shall be the subject of permission validation instead given file.
%%      E.g. for virtual "/" directory returns deafult space file.
%%--------------------------------------------------------------------
-spec get_validation_subject(onedata_user:id(), fslogic_worker:ext_file()) -> fslogic_worker:file() | no_return().
get_validation_subject(UserId, #sfm_handle{file_uuid = FileGUID}) ->
    get_validation_subject(UserId, {guid, FileGUID});
get_validation_subject(UserId, {guid, FileGUID}) ->
    get_validation_subject(UserId, {uuid, fslogic_uuid:file_guid_to_uuid(FileGUID)});
get_validation_subject(UserId, FileEntry) ->
    {ok, #document{key = FileId, value = #file_meta{}} = FileDoc} = file_meta:get(FileEntry),
    FileDoc.

%%--------------------------------------------------------------------
%% @doc Extracts file() from argument list (Inputs) based on Item description.
%%--------------------------------------------------------------------
-spec resolve_file_entry(item_definition(), [term()]) -> fslogic_worker:file().
resolve_file_entry(Item, Inputs) when is_integer(Item) ->
    lists:nth(Item, Inputs);
resolve_file_entry({path, Item}, Inputs) when is_integer(Item) ->
    {path, resolve_file_entry(Item, Inputs)};
resolve_file_entry({parent, Item}, Inputs) ->
    fslogic_utils:get_parent(resolve_file_entry(Item, Inputs)).

%%--------------------------------------------------------------------
%% @doc
%% Expand traverse_ancestors check into list of traverse_container check for
%% each ancestor and subject document
%% @end
%%--------------------------------------------------------------------
-spec expand_traverse_ancestors_check(datastore:document(), datastore:document(), #{}) ->
    [{check_type(), datastore:document(), datastore:document(), [#accesscontrolentity{}]}].
expand_traverse_ancestors_check(SubjDoc = #document{key = Uuid, value = #file_meta{type = Type}},
  UserDoc = #document{key = UserId}, AclMap) ->
    {ok, AncestorsIds} = file_meta:get_ancestors(SubjDoc),
    {Acl, _} = get_acl(Uuid, AclMap),
    SubjectCheck =
        case Type of
            ?DIRECTORY_TYPE ->
                [{?traverse_container, SubjDoc, UserDoc, Acl}];
            _ ->
                []
        end,
    AncestorsCheck =
        lists:map(
            fun(AncestorId) ->
                #document{value = #file_meta{}} = FileDoc = check_permissions:get_validation_subject(UserId, {uuid, AncestorId}),
                {AncestorAcl, _} = get_acl(AncestorId, AclMap),
                {?traverse_container, FileDoc, UserDoc, AncestorAcl}
            end, AncestorsIds),
    SubjectCheck ++ AncestorsCheck.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file has acl defined.
%% @end
%%--------------------------------------------------------------------
-spec has_acl(FileUUID :: file_meta:uuid()) -> boolean().
has_acl(FileUUID) ->
    xattr:exists_by_name(FileUUID, ?ACL_XATTR_NAME).

%%--------------------------------------------------------------------
%% @doc Get acl of given file, returns undefined when acls are empty
%%--------------------------------------------------------------------
-spec get_acl(Uuid :: file_meta:uuid(), Acls :: #{}) -> {[#accesscontrolentity{}] | undefined, #{}}.
get_acl(Uuid, Map) ->
    case maps:get(Uuid, Map, undefined) of
        undefined ->
            case xattr:get_by_name(Uuid, ?ACL_XATTR_NAME) of
                {ok, #document{value = #xattr{value = Value}}} ->
                    AclProplist = json_utils:decode(Value),
                    Acl = fslogic_acl:from_json_fromat_to_acl(AclProplist),
                    {Acl, maps:put(Uuid, Acl, Map)};
                {error, {not_found, xattr}} ->
                    {undefined, maps:put(Uuid, undefined, Map)}
            end;
        Acl ->
            {Acl, Map}
    end.

%%--------------------------------------------------------------------
%% @doc Get file_doc matching given item definition
%%--------------------------------------------------------------------
-spec get_file(item_definition(), #{}, onedata_user:id(), list()) -> {datastore:document(), #{}}.
get_file(ItemDefinition, Map, UserId, FunctionInputs) ->
    case maps:get(ItemDefinition, Map, undefined) of
        undefined ->
            FileDoc  = get_validation_subject(UserId, resolve_file_entry(ItemDefinition, FunctionInputs)),
            {FileDoc, maps:put(ItemDefinition, FileDoc, Map)};
        FileDoc ->
            {FileDoc, Map}
    end.

%%--------------------------------------------------------------------
%% @doc Get user_doc for given user id
%%--------------------------------------------------------------------
-spec get_user(onedata_user:id(), #{}) -> {datastore:document(), #{}}.
get_user(UserId, Map) ->
    case maps:get(UserId, Map, undefined) of
        undefined ->
            {ok, UserDoc} = onedata_user:get(UserId),
            {UserDoc, maps:put(UserId, UserDoc, Map)};
        UserDoc ->
            {UserDoc, Map}
    end.