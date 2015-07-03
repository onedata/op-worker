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
-include("errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([before_advice/4, after_advice/5]).

%% Object pointing to annotation's argument which holds file data (see resolve_file/2)
-type item_definition() :: non_neg_integer() | {path, non_neg_integer()} | {parent, item_definition()}.

-type access_type() :: write | read | exec.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc annotation's before_advice implementation.
%%--------------------------------------------------------------------
-spec before_advice(#annotation{}, atom(), atom(), [term()]) -> term().
%% generic calls
before_advice(#annotation{data = []}, _M, _F, Args) ->
    Args;
before_advice(#annotation{data = [Obj | R]} = A, M, F, [#fslogic_ctx{} | _Inputs] = Args) ->
    ?info("before_advice args ~p", [Args]),
    NewArgs = before_advice(A#annotation{data = Obj}, M, F, Args),
    before_advice(A#annotation{data = R}, M, F, NewArgs);

%% actual before_advice impl.
before_advice(#annotation{}, _M, _F, [#fslogic_ctx{session = #session{identity = #identity{user_id = ?ROOT_USER_ID}}} | _Inputs] = Args) ->
    Args;   %% Always allow access by root user
before_advice(#annotation{data = root}, _M, _F, [#fslogic_ctx{} | _Inputs] = Args) ->
    throw(?EACCES); %% At this point user is not root so deny any requests that require root

before_advice(#annotation{data = {owner, Item}}, _M, _F,
    [#fslogic_ctx{session = #session{identity = #identity{user_id = UserId}}} = Ctx | Inputs] = Args) ->

    #document{value = #file_meta{uid = OwnerId}} = Subj = get_validation_subject(Ctx, resolve_file_entry(Item, Inputs)),

    case UserId of
        OwnerId ->
            ok = validate_ancestors_exec(Subj, UserId),
            Args;
        _       -> throw(?EACCES)
    end;

before_advice(#annotation{data = {none, Item}}, _M, _F,
    [#fslogic_ctx{session = #session{identity = #identity{user_id = UserId}}} = Ctx | Inputs] = Args) ->

    #document{value = #file_meta{}} = Subj = get_validation_subject(Ctx, resolve_file_entry(Item, Inputs)),
    ok = validate_ancestors_exec(Subj, UserId),

    Args;

before_advice(#annotation{data = {owner_if_parent_sticky, Item}}, _M, _F,
    [#fslogic_ctx{session = #session{identity = #identity{user_id = UserId}}} = Ctx | Inputs] = Args) ->

    #document{value = #file_meta{}} = Subj = get_validation_subject(Ctx, resolve_file_entry(Item, Inputs)),
    #document{value = #file_meta{mode = Mode}} = fslogic_utils:get_parent(Subj),

    case (Mode band (8#1 bsl 9)) > 0 of
        true ->
            before_advice(#annotation{data = {owner, Item}}, _M, _F, Args);
        false ->
            Args
    end;

before_advice(#annotation{data = {AccessType, Item}}, _M, _F,
    [#fslogic_ctx{session = #session{identity = #identity{user_id = UserId}}} = Ctx | Inputs] = Args) ->

    #document{value = #file_meta{is_scope = IsScope}} = FileDoc = get_validation_subject(Ctx, resolve_file_entry(Item, Inputs)),

    case IsScope of
        true  ->
            ok = validate_scope_access(AccessType, FileDoc, UserId);
        false ->
            ok
    end,
    
    ok = validate_posix_access(AccessType, FileDoc, UserId),
    ok = validate_ancestors_exec(FileDoc, UserId),

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

-spec get_validation_subject(fslogic_worker:ctx(), FileEntry :: fslogic_worker:file()) -> fslogic_worker:file() | no_return().
get_validation_subject(CTX = #fslogic_ctx{}, FileEntry) ->
    get_validation_subject(fslogic_context:get_user_id(CTX), FileEntry);
get_validation_subject(UserId, FileEntry) ->
    {ok, #document{key = FileId, value = #file_meta{}} = FileDoc} = file_meta:get(FileEntry),
    case FileId of
        UserId ->
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
    ?info("OMG ~p ~p", [Item, Inputs]),
    lists:nth(Item - 1, Inputs);
resolve_file_entry({path, Item}, Inputs) when is_integer(Item) ->
    {path, resolve_file_entry(Item, Inputs)};
resolve_file_entry({parent, Item}, Inputs) ->
    fslogic_utils:get_parent(resolve_file_entry(Item, Inputs)).


-spec validate_posix_access(AccessType :: access_type(), FileDoc :: datastore:document(), UserId :: onedata_user:id()) -> ok | no_return().
validate_scope_access(_AccessType, _FileDoc, _UserId) ->
    ok.

-spec validate_posix_access(AccessType :: access_type(), FileDoc :: datastore:document(), UserId :: onedata_user:id()) -> ok | no_return().
validate_posix_access(AccessType, #document{value = #file_meta{uid = OwnerId, mode = Mode}} = FileDoc, UserId) ->
    ReqBit = case AccessType of
                 read  -> 8#4;
                 write -> 8#2;
                 exec  -> 8#1
             end,

    IsAccessable = case UserId of
                       OwnerId ->
                           ?info("Require ~p to have ~.8B mode on file ~p with mode ~.8B as owner.", [UserId, ReqBit, FileDoc, Mode]),
                           ((ReqBit bsl 6) band Mode) > 0;
                       _ ->
                           {ok, #document{value = #onedata_user{space_ids = Spaces}}} = onedata_user:get(UserId),
                           {ok, #document{key = ScopeUUID}} = file_meta:get_scope(FileDoc),
                           case lists:member(ScopeUUID, Spaces) of
                               true ->
                                   ?info("Require ~p to have ~.8B mode on file ~p with mode ~.8B as space member.", [UserId, ReqBit, FileDoc, Mode]),
                                   ((ReqBit bsl 3) band Mode) > 0;
                               false ->
                                   ?info("Require ~p to have ~.8B mode on file ~p with mode ~.8B as other (Spaces ~p, scope ~p).", [UserId, ReqBit, FileDoc, Mode, Spaces, ScopeUUID]),
                                   (ReqBit band Mode) > 0
                           end
                   end,

    case IsAccessable of
        true    -> ok;
        false   -> throw(?EACCES)
    end.


-spec validate_ancestors_exec(Subj :: fslogic_worker:file(), UserId :: onedata_user:id()) -> ok | no_return().
validate_ancestors_exec(Subj, UserId) ->
    {ok, #document{value = #file_meta{is_scope = IsScope}} = SubjDoc} = file_meta:get(Subj),
    {ok, AncestorsIds} = file_meta:get_ancestors(SubjDoc),
    case IsScope of
        true ->
            ok = validate_posix_access(exec, SubjDoc, UserId);
        false ->
            ok
    end,
    lists:map(
        fun(AncestorId) ->
            #document{value = #file_meta{}} = FileDoc = get_validation_subject(UserId, {uuid, AncestorId}),
            ok = validate_posix_access(exec, FileDoc, UserId)
        end, AncestorsIds),
    ok.