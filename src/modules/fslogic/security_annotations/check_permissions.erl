%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% check_permissions annotation implementation.
%%% This annotation shall check whether annotation's caller has given
%%% permissions to file that is also somewhere within annotation's arguments.
%%% @end
%%%-------------------------------------------------------------------
-module(check_permissions).
-annotation('function').
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([before_advice/4, after_advice/5, get_validation_subject/2]).

%% Object pointing to annotation's argument which holds file data (see resolve_file/2)
-type item_definition() :: non_neg_integer() | {path, non_neg_integer()} | {uuid, file_meta:uuid()} | {parent, item_definition()} | file_ctx:ctx().
-type check_type() :: owner % Check whether user owns the item
| traverse_ancestors % validates ancestors' exec permission.
| owner_if_parent_sticky % Check whether user owns the item but only if parent of the item has sticky bit.
| write | read | exec | rdwr
| acl_access_mask().
-type acl_access_mask() :: binary().
-type access_definition() :: root | {check_type(), item_definition()}.
-type item() :: file_ctx:ctx().

-export_type([check_type/0, item_definition/0, access_definition/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% annotation's before_advice implementation.
%% #annotation.data type is [access_definition()] | access_definition()
%% @end
%%--------------------------------------------------------------------
-spec before_advice(#annotation{data :: [access_definition()]}, module(), atom(), [term()]) -> term().
before_advice(#annotation{data = AccessDefinitions}, _M, _F,
    [#sfm_handle{
        session_id = SessionId,
        file_uuid = FileUuid,
        share_id = ShareId
    } = Handle | RestOfArgs] = Args
) ->
    UserCtx = user_ctx:new(SessionId),
    UserId = user_ctx:get_user_id(UserCtx),
    ExpandedAccessDefinitions = expand_access_definitions(AccessDefinitions, UserId, ShareId, Args),
    [ok = rules:check(Def) || Def <- ExpandedAccessDefinitions],
    case (catch has_acl(FileUuid)) of
        true ->
            [Handle#sfm_handle{session_id = ?ROOT_SESS_ID} | RestOfArgs];
        _ ->
            Args
    end;
before_advice(#annotation{data = AccessDefinitions}, _M, _F, Args = [UserCtx, File | _]) ->
    UserId = user_ctx:get_user_id(UserCtx),
    ShareId = file_ctx:get_share_id_const(File),
    ExpandedAccessDefinitions = expand_access_definitions(AccessDefinitions, UserId, ShareId, Args),
    lists:foreach(fun check_rule_and_cache_result/1, ExpandedAccessDefinitions),
    lists:foreach(fun cache_ok_result/1, ExpandedAccessDefinitions),
    Args.

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
-spec expand_access_definitions([access_definition()], od_user:id(), od_share:id() | undefined, list()) ->
    [{check_type(), undefined | datastore:document(), datastore:document(), od_share:id() | undefined, [#accesscontrolentity{}]}].
expand_access_definitions([], _UserId, _ShareId, _Inputs) ->
    [];
expand_access_definitions(_, ?ROOT_USER_ID, _ShareId, _Inputs) ->
    [];
expand_access_definitions([root | _Rest], _UserId, _ShareId, _Inputs) ->
    throw(?EACCES);
expand_access_definitions(_, ?GUEST_USER_ID, undefined, _Inputs) ->
    throw(?EACCES);
expand_access_definitions([{traverse_ancestors, ItemDefinition} | Rest], UserId, ShareId, Inputs) ->
    User = get_user(UserId),
    {SubjectDoc, ParentDoc} =
        case (catch get_file(ItemDefinition, UserId, Inputs)) of
            Doc = #document{value = #file_meta{is_scope = true}} ->
                {Doc, undefined};
            Doc = #document{} ->
                Parent = get_file({parent, ItemDefinition}, UserId, Inputs),
                {Doc, Parent};
            _ ->
                Parent = get_file({parent, ItemDefinition}, UserId, Inputs),
                {undefined, Parent}
        end,
    expand_traverse_ancestors_check(SubjectDoc, ParentDoc, User, ShareId)
    ++ expand_access_definitions(Rest, UserId, ShareId, Inputs);
expand_access_definitions([{CheckType, ItemDefinition} | Rest], UserId, ShareId, Inputs) ->
    % TODO - do not get document when not needed
    File = #document{key = Key} = get_file(ItemDefinition, UserId, Inputs),
    case permissions_cache:check_permission({CheckType, UserId, ShareId, Key}) of
        {ok, ok} ->
            expand_access_definitions(Rest, UserId, ShareId, Inputs);
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            Acl = get_acl(Key),
            User = get_user(UserId),
            [{CheckType, File, User, ShareId, Acl} | expand_access_definitions(Rest, UserId, ShareId, Inputs)]
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns file that shall be the subject of permission validation instead given file.
%% E.g. for virtual "/" directory returns deafult space file.
%% @end
%%--------------------------------------------------------------------
-spec get_validation_subject(od_user:id(), fslogic_worker:ext_file() | file_ctx:ctx()) ->
    fslogic_worker:file() | no_return().
get_validation_subject(UserId, #sfm_handle{file_uuid = FileUuid}) ->
    get_validation_subject(UserId, {uuid, FileUuid});
get_validation_subject(UserId, {guid, FileGUID}) ->
    get_validation_subject(UserId, {uuid, fslogic_uuid:guid_to_uuid(FileGUID)});
get_validation_subject(_UserId, FileEntry) ->
    case file_ctx:is_file_ctx_const(FileEntry) of
        true ->
            {FileDoc, _NewFileCtx} = file_ctx:get_file_doc(FileEntry),
            FileDoc;
        false ->
            {ok, #document{value = #file_meta{}} = FileDoc} = file_meta:get(FileEntry),
            FileDoc
    end.

%%--------------------------------------------------------------------
%% @doc
%% Extracts file() from argument list (Inputs) based on Item description.
%% @end
%%--------------------------------------------------------------------
-spec resolve_file_entry(item_definition(), [term()]) -> fslogic_worker:file() | file_ctx:ctx().
resolve_file_entry(Item, Inputs) when is_integer(Item) ->
    lists:nth(Item, Inputs);
resolve_file_entry({path, Item}, Inputs) when is_integer(Item) ->
    {path, resolve_file_entry(Item, Inputs)};
resolve_file_entry({parent, Item}, Inputs) ->
    ResolvedEntry = resolve_file_entry(Item, Inputs),
    case file_ctx:is_file_ctx_const(ResolvedEntry) of
        true ->
            {Parent, _NewFileCtx} = file_ctx:get_parent(ResolvedEntry, undefined),
            Parent;
        false ->
            fslogic_utils:get_parent(ResolvedEntry)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Expand traverse_ancestors check into list of traverse_container check for
%% each ancestor and subject document
%% @end
%%--------------------------------------------------------------------
-spec expand_traverse_ancestors_check(file_meta:doc() | undefined, file_meta:doc() | undefined,
    od_user:doc(), od_share:id() | undefined) ->
    [{check_type(), datastore:document(), datastore:document(), od_share:id() | undefined, [#accesscontrolentity{}]}].
expand_traverse_ancestors_check(SubjectDoc, ParentDoc, UserDoc = #document{key = UserId}, ShareId) ->
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
                        Acl = get_acl(Uuid),
                        [{?traverse_container, NewSubjDoc, UserDoc, ShareId, Acl}]
                end;
            _ ->
                []
        end,

    {AncestorsCheck, CacheUsed} = expand_ancestors_check(Uuid, [], UserId, UserDoc, ShareId),
    case {ShareId, CacheUsed} of
        {undefined, _} ->
            SubjectCheck ++ AncestorsCheck;
        {_, true} ->
            SubjectCheck ++ AncestorsCheck;
        {_, false} ->
            PotentialShares = [SubjectDoc, NewSubjDoc] ++ [Doc || {_, Doc, _, _, _} <- AncestorsCheck],
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
-spec expand_ancestors_check(file_meta:uuid(), Acc, od_user:id(), od_user:doc(), od_share:id() | undefined) ->
    {Acc, CacheUsed :: boolean()} when
    Acc :: [{acl_access_mask(), file_meta:doc(), od_user:doc(), od_share:id() | undefined, undefined | [#accesscontrolentity{}]}].
expand_ancestors_check(?ROOT_DIR_UUID, Acc, _UserId, _UserDoc, _ShareId) ->
    {Acc, false};
expand_ancestors_check(Key, Acc, UserId, UserDoc, ShareId) ->
    {ok, AncestorId} = file_meta:get_parent_uuid_in_context(Key),
    case permissions_cache:check_permission({?traverse_container, UserId, ShareId, AncestorId}) of
        {ok, ok} ->
            {Acc, true};
        {ok, ?EACCES} ->
            throw(?EACCES);
        _ ->
            #document{value = #file_meta{}} = FileDoc = check_permissions:get_validation_subject(UserId, {uuid, AncestorId}),
            AncestorAcl = get_acl(AncestorId),
            expand_ancestors_check(AncestorId, [{?traverse_container, FileDoc, UserDoc, ShareId, AncestorAcl} | Acc],
                UserId, UserDoc, ShareId)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if file has acl defined.
%% @end
%%--------------------------------------------------------------------
-spec has_acl(file_meta:uuid()) -> boolean().
has_acl(FileUuid) ->
    xattr:exists_by_name(FileUuid, ?ACL_XATTR_NAME).

%%--------------------------------------------------------------------
%% @doc
%% Get acl of given file, returns undefined when acls are empty
%% @end
%%--------------------------------------------------------------------
-spec get_acl(file_meta:uuid()) -> [#accesscontrolentity{}].
get_acl(Uuid) ->
    case xattr:get_by_name(Uuid, ?ACL_XATTR_NAME) of
        {ok, Value} ->
            fslogic_acl:from_json_format_to_acl(Value);
        {error, {not_found, custom_metadata}} ->
            undefined
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get file_doc matching given item definition
%% @end
%%--------------------------------------------------------------------
-spec get_file(item_definition(), od_user:id(), list()) -> {datastore:document(), maps:map()}.
get_file(ItemDefinition, UserId, FunctionInputs) ->
    get_validation_subject(UserId, resolve_file_entry(ItemDefinition, FunctionInputs)).

%%--------------------------------------------------------------------
%% @doc
%% Get user_doc for given user id
%% @end
%%--------------------------------------------------------------------
-spec get_user(od_user:id()) -> od_user:doc().
get_user(UserId) ->
    {ok, UserDoc} = od_user:get(UserId),
    UserDoc.

%%--------------------------------------------------------------------
%% @doc
%% Check rule and cache result
%% @end
%%--------------------------------------------------------------------
-spec check_rule_and_cache_result({term(), FileDoc :: datastore:document() | undefined,
    UserDoc :: datastore:document(), ShareId :: od_share:id() | undefined, Acl :: [#accesscontrolentity{}] | undefined}) -> ok.
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
    od_user:doc(), od_share:id() | undefined, Acl :: [#accesscontrolentity{}] | undefined}) -> ok.
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

