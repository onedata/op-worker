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
    Args = [#fslogic_ctx{session = #session{identity = #user_identity{user_id = UserId}}, share_id = ShareId} | _]) ->
    ExpandedAccessDefinitions = expand_access_definitions(AccessDefinitions, UserId, ShareId, Args, #{}, #{}, #{}),
    % TODO - beter cache "or" in AccessType
    % TODO - better cache EACCES (it will be always traversed to first EACCES)
    lists:foreach(fun check_rule_and_cache_result/1, ExpandedAccessDefinitions),
    lists:foreach(fun cache_ok_result/1, ExpandedAccessDefinitions),
    Args;
before_advice(#annotation{data = AccessDefinitions}, _M, _F, [#sfm_handle{session_id = SessionId, file_uuid = FileUUID, share_id = ShareId} = Handle | RestOfArgs] = Args) ->
    {ok, #document{value = #session{identity = #user_identity{user_id = UserId}}}} = session:get(SessionId),
    ExpandedAccessDefinitions = expand_access_definitions(AccessDefinitions, UserId, ShareId, Args, #{}, #{}, #{}),
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Expand access definition to form allowing it to be verified by rules module.
%% @end
%%--------------------------------------------------------------------
-spec expand_access_definitions([access_definition()], onedata_user:id(), share_info:id(), list(), maps:map(), maps:map(), maps:map()) ->
    [{check_type(), undefined | datastore:document(), datastore:document(), share_info:id(), [#accesscontrolentity{}]}].
expand_access_definitions([], _UserId, _ShareId, _Inputs, _FileMap, _AclMap, _UserMap) ->
    [];
expand_access_definitions(_, ?ROOT_USER_ID, _ShareId, _Inputs, _FileMap, _AclMap, _UserMap) ->
    [];
expand_access_definitions([root | _Rest], _UserId, _ShareId, _Inputs, _FileMap, _AclMap, _UserMap) ->
    throw(?EACCES);
expand_access_definitions(_, ?GUEST_USER_ID, undefined, _Inputs, _FileMap, _AclMap, _UserMap) ->
    throw(?EACCES);
expand_access_definitions([{traverse_ancestors, ItemDefinition} | Rest], UserId, ShareId, Inputs, FileMap, AclMap, UserMap) ->
    {User, NewUserMap} = get_user(UserId, UserMap),
    {SubjectDoc, ParentDoc, NewFileMap} =
        case (catch get_file(ItemDefinition, FileMap, UserId, Inputs)) of
            {Doc = #document{value = #file_meta{is_scope = true}}, Map} ->
                {Doc, undefined, Map};
            {Doc = #document{}, Map} ->
                {Parent, Map2} = get_file({parent, ItemDefinition}, Map, UserId, Inputs),
                {Doc, Parent, Map2};
            _ ->
                {Parent, Map} = get_file({parent, ItemDefinition}, FileMap, UserId, Inputs),
                {undefined, Parent, Map}
        end,
    expand_traverse_ancestors_check(SubjectDoc, ParentDoc, User, ShareId, AclMap)
    ++ expand_access_definitions(Rest, UserId, ShareId, Inputs, NewFileMap, AclMap, NewUserMap);
expand_access_definitions([{CheckType, ItemDefinition} | Rest], UserId, ShareId, Inputs, FileMap, AclMap, UserMap) ->
    % TODO - do not get document when not needed
    {File = #document{key = Key}, NewFileMap} = get_file(ItemDefinition, FileMap, UserId, Inputs),
    case permissions_cache:check_permission({CheckType, UserId, ShareId, Key}) of
        {ok, ok} ->
            expand_access_definitions(Rest, UserId, ShareId, Inputs, NewFileMap, AclMap, UserMap);
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            {Acl, NewAclMap} = get_acl(Key, AclMap),
            {User, NewUserMap} = get_user(UserId, UserMap),
            [{CheckType, File, User, ShareId, Acl} | expand_access_definitions(Rest, UserId, ShareId, Inputs, NewFileMap, NewAclMap, NewUserMap)]
    end.


%%--------------------------------------------------------------------
%% @doc Returns file that shall be the subject of permission validation instead given file.
%%      E.g. for virtual "/" directory returns deafult space file.
%%--------------------------------------------------------------------
-spec get_validation_subject(onedata_user:id(), fslogic_worker:ext_file()) -> fslogic_worker:file() | no_return().
get_validation_subject(UserId, #sfm_handle{file_uuid = FileGUID}) ->
    get_validation_subject(UserId, {guid, FileGUID});
get_validation_subject(UserId, {guid, FileGUID}) ->
    get_validation_subject(UserId, {uuid, fslogic_uuid:guid_to_uuid(FileGUID)});
get_validation_subject(_UserId, FileEntry) ->
    {ok, #document{value = #file_meta{}} = FileDoc} = file_meta:get(FileEntry),
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
-spec expand_traverse_ancestors_check(file_meta:doc() | undefined, file_meta:doc() | undefined,
    onedata_user:doc(), share_info:id(), maps:map()) ->
    [{check_type(), datastore:document(), datastore:document(), share_info:id(), [#accesscontrolentity{}]}].
expand_traverse_ancestors_check(SubjectDoc, ParentDoc,
    UserDoc = #document{key = UserId}, ShareId, AclMap) ->

    NewSubjDoc =
        case SubjectDoc =/= undefined andalso SubjectDoc#document.value#file_meta.is_scope of
            true ->
                SubjectDoc;
            false ->
                ParentDoc
        end,
    #document{key = Uuid, value = #file_meta{type = Type}} = NewSubjDoc,
    SubjectCheck =
        case Type of
            ?DIRECTORY_TYPE ->
                case permissions_cache:check_permission({?traverse_container, UserId, ShareId, Uuid}) of
                    {ok, ok} ->
                        [];
                    {ok, ?EACCES} ->
                        throw(?EACCES);
                    _ ->
                        {Acl, _} = get_acl(Uuid, AclMap),
                        [{?traverse_container, NewSubjDoc, UserDoc, ShareId, Acl}]
                end;
            _ ->
                []
        end,

    file_meta:set_link_context(NewSubjDoc),
    {AncestorsCheck, CacheUsed} = expand_ancestors_check(Uuid, [], UserId, UserDoc, ShareId, AclMap),
    case {ShareId, CacheUsed} of
        {undefined, _} ->
            ?critical("@@@ Share undefined"),
            SubjectCheck ++ AncestorsCheck;
        {_, true} ->
            ?critical("@@@ Share defined, cache used"),
            SubjectCheck ++ AncestorsCheck;
        {_, false} ->
            ?critical("@@@ Share defined, cache not used"),

            PotentialShares = [SubjectDoc, NewSubjDoc] ++ [Doc || {_, Doc, _, _, _} <- AncestorsCheck],
            ?critical("potential shares ~p", [PotentialShares]),
            IsValidShare = lists:any(
                fun(#document{value = #file_meta{shares = Shares}}) ->
                    lists:member(ShareId, Shares)
                end, PotentialShares),
            case IsValidShare of
                true ->
                    SubjectCheck ++ AncestorsCheck;
                false ->
                    throw(?EACCES)
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Expand traverse_ancestors check into list of traverse_container check for
%% each ancestor and subject document. Starts from doc parent.
%% @end
%%--------------------------------------------------------------------
-spec expand_ancestors_check(file_meta:uuid(), Acc, onedata_user:id(), onedata_user:doc(), share_info:id(), maps:map()) ->
    {Acc, CacheUsed :: boolean()} when
    Acc :: [{acl_access_mask(), file_meta:doc(), onedata_user:doc(), share_info:id(), undefined | [#accesscontrolentity{}]}].
expand_ancestors_check(?ROOT_DIR_UUID, Acc, _UserId, _UserDoc, _ShareId, _AclMap) ->
    {Acc, false};
expand_ancestors_check(Key, Acc, UserId, UserDoc, ShareId, AclMap) ->
    {ok, AncestorId} = file_meta:get_parent_uuid_in_context(Key),
    case permissions_cache:check_permission({?traverse_container, UserId, ShareId, AncestorId}) of
        {ok, ok} ->
            {Acc, true};
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            #document{value = #file_meta{}} = FileDoc = check_permissions:get_validation_subject(UserId, {uuid, AncestorId}),
            {AncestorAcl, _} = get_acl(AncestorId, AclMap),
            expand_ancestors_check(AncestorId, [{?traverse_container, FileDoc, UserDoc, ShareId, AncestorAcl} | Acc],
                UserId, UserDoc, ShareId, AclMap)
    end.

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
-spec get_acl(Uuid :: file_meta:uuid(), Acls :: maps:map()) -> {[#accesscontrolentity{}] | undefined, maps:map()}.
get_acl(Uuid, Map) ->
    case maps:get(Uuid, Map, undefined) of
        undefined ->
            case xattr:get_by_name(Uuid, ?ACL_XATTR_NAME) of
                {ok, Value} ->
                    AclMap = json_utils:decode_map(Value),
                    Acl = fslogic_acl:from_json_format_to_acl(AclMap),
                    {Acl, maps:put(Uuid, Acl, Map)};
                {error, {not_found, custom_metadata}} ->
                    {undefined, maps:put(Uuid, undefined, Map)}
            end;
        Acl ->
            {Acl, Map}
    end.

%%--------------------------------------------------------------------
%% @doc Get file_doc matching given item definition
%%--------------------------------------------------------------------
-spec get_file(item_definition(), maps:map(), onedata_user:id(), list()) -> {datastore:document(), maps:map()}.
get_file(ItemDefinition, Map, UserId, FunctionInputs) ->
    case maps:get(ItemDefinition, Map, undefined) of
        undefined ->
            FileDoc = get_validation_subject(UserId, resolve_file_entry(ItemDefinition, FunctionInputs)),
            {FileDoc, maps:put(ItemDefinition, FileDoc, Map)};
        FileDoc ->
            {FileDoc, Map}
    end.

%%--------------------------------------------------------------------
%% @doc Get user_doc for given user id
%%--------------------------------------------------------------------
-spec get_user(onedata_user:id(), maps:map()) -> {datastore:document(), maps:map()}.
get_user(UserId, Map) ->
    case maps:get(UserId, Map, undefined) of
        undefined ->
            {ok, UserDoc} = onedata_user:get(UserId),
            {UserDoc, maps:put(UserId, UserDoc, Map)};
        UserDoc ->
            {UserDoc, Map}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check rule and cache result
%% @end
%%--------------------------------------------------------------------
-spec check_rule_and_cache_result({term(), FileDoc :: datastore:document() | undefined,
    UserDoc :: datastore:document(), ShareId :: share_info:id(), Acl :: [#accesscontrolentity{}] | undefined}) -> ok.
check_rule_and_cache_result({AccessType, FileDoc, UserDoc, ShareId, _} = Def) ->
    try
        ok = rules:check(Def)
    catch
        _:?EACCES ->
            permissions_cache:cache_permission({AccessType, get_doc_id(UserDoc), ShareId, get_doc_id(FileDoc)}, ?EACCES),
            throw(?EACCES)
    end.
%%--------------------------------------------------------------------
%% @doc
%% Cache given rules result check as 'ok'
%% @end
%%--------------------------------------------------------------------
-spec cache_ok_result({CheckType :: term(), file_meta:doc() | undefined,
    onedata_user:doc(), share_info:id(), Acl :: [#accesscontrolentity{}] | undefined}) -> ok.
cache_ok_result({AccessType, FileDoc, UserDoc, ShareId, _}) ->
    permissions_cache:cache_permission({AccessType, get_doc_id(UserDoc), ShareId, get_doc_id(FileDoc)}, ok).

%%--------------------------------------------------------------------
%% @doc
%% Get doc id or return undefined
%% @end
%%--------------------------------------------------------------------
-spec get_doc_id(datastore:document()) -> datastore:key().
get_doc_id(undefined) ->
    undefined;
get_doc_id(#document{key = Key}) ->
    Key.

