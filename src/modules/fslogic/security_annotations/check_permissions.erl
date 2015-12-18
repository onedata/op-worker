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
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([before_advice/4, after_advice/5, get_validation_subject/2, get_acl/1]).

%% Object pointing to annotation's argument which holds file data (see resolve_file/2)
-type item_definition() :: non_neg_integer() | {path, non_neg_integer()} | {uuid, file_meta:uuid()} | {parent, item_definition()}.
-type check_type() :: owner % Check whether user owns the item
                    | traverse_ancestors % validates ancestors' exec permission.
                    | owner_if_parent_sticky % Check whether user owns the item but only if parent of the item has sticky bit.
                    | write
                    | read
                    | exec
                    | rdwr
                    | term(). %todo define as acl access mask


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
%% generic calls
before_advice(#annotation{data = []}, _M, _F, Args) ->
    Args;
before_advice(#annotation{data = [{CheckType, ItemDefinition} | Rest]} = Annotation, M, F,
  [#fslogic_ctx{session = #session{identity = #identity{user_id = UserId}}} = Ctx | Inputs] = Args) ->
    FileDoc = #document{key = Key} = get_validation_subject(Ctx, resolve_file_entry(ItemDefinition, Inputs)),
    Acl = get_acl(Key), %todo refactor and get it once
    {ok, UserDoc} = onedata_user:get(UserId), %todo refactor and get it once
    ok = rules:check({CheckType, FileDoc}, UserDoc, Acl),
    before_advice(Annotation#annotation{data = Rest}, M, F, Args);
before_advice(#annotation{data = [AccessDefinition | Rest]} = Annotation, M, F,
  [#fslogic_ctx{session = #session{identity = #identity{user_id = UserId}}} | _] = Args) ->
    {ok, UserDoc} = onedata_user:get(UserId), %todo refactor and get it once
    ok = rules:check(AccessDefinition, UserDoc, undefined),
    before_advice(Annotation#annotation{data = Rest}, M, F, Args).

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
%% @doc Returns file that shall be the subject of permission validation instead given file.
%%      E.g. for virtual "/" directory returns deafult space file.
%%--------------------------------------------------------------------
-spec get_validation_subject(fslogic_worker:ctx(), FileEntry :: fslogic_worker:file()) -> fslogic_worker:file() | no_return().
get_validation_subject(CTX = #fslogic_ctx{}, FileEntry) ->
    get_validation_subject(fslogic_context:get_user_id(CTX), FileEntry);
get_validation_subject(UserId, FileEntry) ->
    {ok, #document{key = FileId, value = #file_meta{}} = FileDoc} = file_meta:get(FileEntry),
    DefaultSpaceDirUuid = fslogic_uuid:default_space_uuid(UserId),
    case FileId of
        DefaultSpaceDirUuid ->
            {ok, #document{} = SpaceDoc} = fslogic_spaces:get_default_space(UserId),
            SpaceDoc;
        _ ->
            FileDoc
    end.

%%--------------------------------------------------------------------
%% @doc Extracts file() from argument list (Inputs) based on Item description.
%%--------------------------------------------------------------------
-spec resolve_file_entry(item_definition(), [term()]) -> fslogic_worker:file().
resolve_file_entry(Item, Inputs) when is_integer(Item) ->
    lists:nth(Item - 1, Inputs);
resolve_file_entry({path, Item}, Inputs) when is_integer(Item) ->
    {path, resolve_file_entry(Item, Inputs)};
resolve_file_entry({parent, Item}, Inputs) ->
    fslogic_utils:get_parent(resolve_file_entry(Item, Inputs)).

%%--------------------------------------------------------------------
%% @doc Get acl of given file, returns undefined when acls are empty
%%--------------------------------------------------------------------
-spec get_acl(Uuid :: file_meta:uuid()) -> [#accesscontrolentity{}] | undefined.
get_acl(Uuid) -> %todo do not export
    case xattr:get_by_name(Uuid, ?ACL_XATTR_NAME) of
        {ok, #document{value = #xattr{value = Value}}} ->
            AclProplist = json_utils:decode(Value),
            fslogic_acl:from_json_fromat_to_acl(AclProplist);
        {error, {not_found, xattr}} ->
            undefined
    end.
