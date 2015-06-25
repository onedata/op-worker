%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for file's metadata. Implemets low-level metadata operations such as
%%%      walking through file graph.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta).
-author("Rafal Slota").
-behaviour(model_behaviour).

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_spaces.hrl").

%% Runs given codeblock and converts any badmatch/case_clause to {error, Reason :: term()}
-define(run(B),
    try B of
        __Other -> __Other
    catch
        error:__Reason ->
            __Reason0 = normalize_error(__Reason),
            ?error_stacktrace("file_meta error: ~p", [__Reason0]),
            {error, __Reason0}
    end).

%% How many processes shall be process single set_scope operation.
-define(SET_SCOPER_WORKERS, 25).

%% How many entries shall be processed in one batch for set_scope operation.
-define(SET_SCOPE_BATCH_SIZE, 100).

-define(ROOT_DIR_UUID, <<"">>).
-define(ROOT_DIR_NAME, <<"">>).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).

-export([resolve_path/1, create/2, get_scope/1, list_children/3, get_parent/1,
    gen_path/1, rename/2, setup_onedata_user/1]).

-type uuid() :: datastore:key().
-type path() :: binary().
-type name() :: binary().
-type entry() :: {path, path()} | {uuid, uuid()} | datastore:document().
-type type() :: ?REGULAR_FILE_TYPE | ?DIRECTORY_TYPE | ?LINK_TYPE.
-type offset() :: non_neg_integer().
-type size() :: non_neg_integer().
-type mode() :: non_neg_integer().
-type time() :: non_neg_integer().
-type file_meta() :: model_name().
-type posix_permissions() :: non_neg_integer().

-export_type([uuid/0, path/0, name/0, entry/0, type/0, offset/0, size/0, mode/0,
    time/0, posix_permissions/0]).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, uuid()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(uuid() | entry(), Diff :: datastore:document_diff()) ->
    {ok, uuid()} | datastore:update_error().
update({uuid, Key}, Diff) ->
    update(Key, Diff);
update(#document{value = #file_meta{}, key = Key}, Diff) ->
    update(Key, Diff);
update({path, Path}, Diff) ->
    ?run(begin
             {ok, {#document{} = Document, _}} = resolve_path(Path),
             update(Document, Diff)
         end);
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, uuid()} | datastore:create_error().
create(#document{value = #file_meta{name = FileName}} = Document) ->
    case is_valid_filename(FileName) of
        true ->
            datastore:create(?STORE_LEVEL, Document);
        false ->
            {error, invalid_filename}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates new #file_meta and links it as a new child of given as first argument existing #file_meta.
%% @end
%%--------------------------------------------------------------------
-spec create(entry(), file_meta() | datastore:document()) -> {ok, uuid()} | datastore:create_error().
create({uuid, ParentUUID}, File) ->
    ?run(begin
             {ok, Parent} = get(ParentUUID),
             create(Parent, File)
         end);
create({path, Path}, File) ->
    ?run(begin
             {ok, {Parent, _}} = resolve_path(Path),
             create(Parent, File)
         end);
create(#document{} = Parent, #file_meta{} = File) ->
    create(Parent, #document{value = File});
create(#document{} = Parent, #document{value = #file_meta{name = FileName}} = FileDoc) ->
    ?run(begin
             case create(FileDoc) of
                 {ok, UUID} ->
                     SavedDoc = FileDoc#document{key = UUID},
                     {ok, Scope} = get_scope(Parent),
                     ok = datastore:add_links(?LINK_STORE_LEVEL, Parent, {FileName, SavedDoc}),
                     ok = datastore:add_links(?LINK_STORE_LEVEL, SavedDoc, [{parent, Parent}, {scope, Scope}]),
                     {ok, UUID};
                 {error, Reason} ->
                     {error, Reason}
             end
         end).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(uuid() | entry()) -> {ok, datastore:document()} | datastore:get_error().
get({uuid, Key}) ->
    get(Key);
get(#document{value = #file_meta{}} = Document) ->
    {ok, Document};
get({path, Path}) ->
    ?run(begin
             {ok, {Doc, _}} = resolve_path(Path),
             {ok, Doc}
         end);
get(?ROOT_DIR_UUID) ->
    {ok, #document{key = ?ROOT_DIR_UUID, value =
    #file_meta{name = ?ROOT_DIR_NAME, is_scope = true}}};
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(uuid() | entry()) -> ok | datastore:generic_error().
delete({uuid, Key}) ->
    delete(Key);
delete(#document{value = #file_meta{name = FileName}, key = Key}) ->
    ?run(begin
             case datastore:fetch_link(?LINK_STORE_LEVEL, Key, ?MODEL_NAME, parent) of
                 {ok, {ParentKey, ?MODEL_NAME}} ->
                     ok = datastore:delete_links(?LINK_STORE_LEVEL, ParentKey, ?MODEL_NAME, FileName);
                 _ ->
                     ok
             end,
             datastore:delete(?STORE_LEVEL, ?MODULE, Key)
         end);
delete({path, Path}) ->
    ?run(begin
             {ok, {#document{} = Document, _}} = resolve_path(Path),
             delete(Document)
         end);
delete(Key) ->
    ?run(begin
             {ok, #document{} = Document} = get(Key),
             delete(Document)
         end).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(uuid() | entry()) -> datastore:exists_return().
exists({uuid, Key}) ->
    exists(Key);
exists(#document{value = #file_meta{}, key = Key}) ->
    exists(Key);
exists({path, Path}) ->
    case resolve_path(Path) of
        {ok, {#document{}, _}} ->
            true;
        {error, {not_found, _}} ->
            false;
        {error, ghost_file} ->
            false;
        {error, link_not_found} ->
            false
    end;
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(files, [{onedata_user, create}], ?GLOBALLY_CACHED_LEVEL, ?GLOBALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(onedata_user, create, _, _, {ok, UUID}) ->
    setup_onedata_user(UUID);
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) ->
    ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Lists children of given #file_meta.
%% @end
%%--------------------------------------------------------------------
-spec list_children(Entry :: entry(), Offset :: non_neg_integer(), Count :: non_neg_integer()) ->
    {ok, [#child_link{}]} | {error, Reason :: term()}.
list_children(Entry, Offset, Count) ->
    ?run(begin
             {ok, #document{} = File} = get(Entry),
             Res = datastore:foreach_link(?LINK_STORE_LEVEL, File, fun
                 (_LinkName, _LinkTarget, {_, 0, _} = Acc) ->
                     Acc;
                 (LinkName, {_Key, ?MODEL_NAME}, {Skip, Count1, Acc}) when is_binary(LinkName), Skip > 0 ->
                     {Skip - 1, Count1, Acc};
                 (LinkName, {Key, ?MODEL_NAME}, {0, Count1, Acc}) when is_binary(LinkName), Count > 0 ->
                     {0, Count1 - 1, [#child_link{uuid = Key, name = LinkName} | Acc]};
                 (_LinkName, _LinkTarget, AccIn) ->
                     AccIn
             end, {Offset, Count, []}),
             case Res of
                 {ok, {_, _, UUIDs}} ->
                     {ok, UUIDs};
                 {error, Reason} ->
                     {error, Reason}
             end
         end).

%%--------------------------------------------------------------------
%% @doc
%% Returns file's parent document.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(Entry :: entry()) -> {ok, datastore:document()} | datastore:get_error().
get_parent(Entry) ->
    ?run(begin
             {ok, #document{key = Key}} = get(Entry),
             {ok, {ParentKey, ?MODEL_NAME}} =
                 datastore:fetch_link(?LINK_STORE_LEVEL, Key, ?MODEL_NAME, parent),
             get({uuid, ParentKey})
         end).

%%--------------------------------------------------------------------
%% @doc
%% Generate file_meta:path() for given file_meta:entry()
%% @end
%%--------------------------------------------------------------------
-spec gen_path(entry()) -> {ok, path()} | datastore:generic_error().
gen_path({path, Path}) when is_binary(Path) ->
    {ok, Path};
gen_path(Entry) ->
    ?run(begin
             gen_path2(Entry, [])
         end).

%%--------------------------------------------------------------------
%% @doc
%% Resolves given file_meta:path() and returns file_meta:entry() along with list of
%% all ancestors' UUIDs.
%% @end
%%--------------------------------------------------------------------
-spec resolve_path(path()) -> {ok, {datastore:document(), [uuid()]}} | datastore:generic_error().
resolve_path(<<?DIRECTORY_SEPARATOR, Path/binary>>) ->
    ?run(begin
             case fslogic_path:split(Path) of
                 [] ->
                     {ok, #document{key = RootUUID} = Root} = get(?ROOT_DIR_UUID),
                     {ok, {Root, [RootUUID]}};
                 Tokens ->
                     case datastore:link_walk(?LINK_STORE_LEVEL, ?RESPONSE(get(?ROOT_DIR_UUID)), Tokens, get_leaf) of
                         {ok, {Leaf, KeyPath}} ->
                             [_ | [RealParentUUID | _]] = lists:reverse([?ROOT_DIR_UUID | KeyPath]),
                             {ok, {ParentUUID, _}} = datastore:fetch_link(?LINK_STORE_LEVEL, Leaf, parent),
                             case ParentUUID of
                                 RealParentUUID ->
                                     {ok, {Leaf, [?ROOT_DIR_UUID | KeyPath]}};
                                 _ ->
                                     {error, ghost_file}
                             end;
                         {error, link_not_found} -> %% Map links errors to document errors
                             {error, {not_found, ?MODEL_NAME}};
                         {error, Reason} ->
                             {error, Reason}
                     end
             end
         end).

%%--------------------------------------------------------------------
%% @doc
%% Moves given file to specific location. Move operation ({path, _}) is more generic, but
%% rename using simple file name ({name, _}) is faster because it does not change parent of the file.
%% @end
%%--------------------------------------------------------------------
-spec rename(entry(), {name, name()} | {path, path()}) -> ok | datastore:generic_error().
rename({path, Path}, Op) ->
    ?run(begin
             {ok, {Subj, KeyPath}} = resolve_path(Path),
             [_ | [ParentUUID | _]] = lists:reverse(KeyPath),
             rename3(Subj, ParentUUID, Op)
         end);
rename(Entry, Op) ->
    ?run(begin
             {ok, Subj} = get(Entry),
             {ok, {ParentUUID, _}} = datastore:fetch_link(?LINK_STORE_LEVEL, Subj, parent),
             rename3(Subj, ParentUUID, Op)
         end).

%%--------------------------------------------------------------------
%% @doc
%% Gets "scope" document of given document. "Scope" document is the nearest ancestor with #file_meta.is_scope == true.
%% @end
%%--------------------------------------------------------------------
-spec get_scope(Entry :: entry()) -> {ok, ScopeDoc :: datastore:document()} | datastore:generic_error().
get_scope(#document{value = #file_meta{is_scope = true}} = Document) ->
    {ok, Document};
get_scope(#document{value = #file_meta{is_scope = false}} = Document) ->
    datastore:fetch_link_target(?LINK_STORE_LEVEL, Document, scope);
get_scope(Entry) ->
    ?run(begin
             {ok, Doc} = get(Entry),
             get_scope(Doc)
         end).

%%--------------------------------------------------------------------
%% @doc
%% Initializes files metadata for onedata user.
%% @end
%%--------------------------------------------------------------------
-spec setup_onedata_user(UUID :: uuid()) -> ok.
setup_onedata_user(UUID) ->
    try
        {ok, #document{value = #onedata_user{space_ids = [DefaultSpaceId | _]}}} =
            onedata_user:get(UUID),
        {ok, #space_details{name = DefaultSpaceName}} =
            gr_spaces:get_details(provider, DefaultSpaceId),

        CTime = utils:time(),
        {ok, RootUUID} = create({uuid, ?ROOT_DIR_UUID}, #document{key = UUID,
            value = #file_meta{
                name = UUID, type = ?DIRECTORY_TYPE, mode = 8#644,
                mtime = CTime, atime = CTime, ctime = CTime, uid = UUID
            }
        }),
        {ok, SpacesUUID} = create({uuid, RootUUID}, #file_meta{
            name = ?SPACES_BASE_DIR_NAME, type = ?DIRECTORY_TYPE, mode = 8#644,
            mtime = CTime, atime = CTime, ctime = CTime, uid = UUID
        }),
        {ok, _} = create({uuid, SpacesUUID}, #file_meta{
            name = DefaultSpaceName, type = ?DIRECTORY_TYPE, mode = 8#644,
            mtime = CTime, atime = CTime, ctime = CTime, uid = UUID
        }),

        ok
    catch
        Error:Reason ->
            ?error_stacktrace("Cannot initialize onedata user files metadata "
            "due to: ~p:~p", [Error, Reason])
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Internel helper function for rename/2.
%% @end
%%--------------------------------------------------------------------
-spec rename3(Subject :: datastore:document(), ParentUUID :: uuid(), {name, NewName :: name()} | {path, NewPath :: path()}) ->
    ok | datastore:generic_error().
rename3(#document{value = #file_meta{name = OldName}} = Subject, ParentUUID, {name, NewName}) ->
    ?run(begin
             {ok, FileUUID} = update(Subject, #{name => NewName}),
             ok = datastore:add_links(?LINK_STORE_LEVEL, ParentUUID, ?MODEL_NAME, {NewName, {FileUUID, ?MODEL_NAME}}),
             ok = datastore:delete_links(?LINK_STORE_LEVEL, ParentUUID, ?MODEL_NAME, OldName),
             {ok, FileUUID}
         end);
rename3(#document{value = #file_meta{name = OldName}} = Subject, OldParentUUID, {path, NewPath}) ->
    ?run(begin
             NewTokens = fslogic_path:split(NewPath),
             [NewName | NewParentTokens] = lists:reverse(NewTokens),
             NewParentPath = fslogic_path:join(lists:reverse(NewParentTokens)),
             {ok, NewParent} = get({path, NewParentPath}),

             {ok, NewScope} = get_scope(NewParent),

             ok = datastore:add_links(?LINK_STORE_LEVEL, NewParent, {NewName, Subject}),
             {ok, FileUUID} = update(Subject, #{name => NewName}),
             ok = datastore:delete_links(?LINK_STORE_LEVEL, OldParentUUID, ?MODEL_NAME, OldName),
             ok = datastore:add_links(?LINK_STORE_LEVEL, FileUUID, ?MODEL_NAME, {parent, NewParent}),

             ok = update_scopes(Subject, NewScope),

             {ok, FileUUID}
         end).

%%--------------------------------------------------------------------
%% @doc
%% Force set "scope" document for given file_meta:entry() and all its children recursively but only if
%% given file_meta:entry() has differen "scope" document.
%% @end
%%--------------------------------------------------------------------
-spec update_scopes(Entry :: entry(), NewScope :: datastore:document()) -> ok | datastore:generic_error().
update_scopes(Entry, #document{key = NewScopeUUID} = NewScope) ->
    ?run(begin
             {ok, #document{key = OldScopeUUID}} = get_scope(Entry),
             case OldScopeUUID of
                 NewScopeUUID -> ok;
                 _ ->
                     set_scopes(Entry, NewScope)
             end
         end).

%%--------------------------------------------------------------------
%% @doc
%% Force set "scope" document for given file_meta:entry() and all its children recursively.
%% @end
%%--------------------------------------------------------------------
-spec set_scopes(entry(), datastore:document()) -> ok | datastore:generic_error().
set_scopes(Entry, #document{key = NewScopeUUID}) ->
    ?run(begin
             SetterFun =
                 fun(CurrentEntry, ScopeUUID) ->
                     {ok, CurrentUUID} = to_uuid(CurrentEntry),
                     ok = datastore:add_links(?LINK_STORE_LEVEL, CurrentUUID, ?MODEL_NAME, {scope, {ScopeUUID, ?MODEL_NAME}})
                 end,

             ReceiverFun =
                 fun Receiver() ->
                     receive
                         {Entry0, ScopeUUID0} ->
                             SetterFun(Entry0, ScopeUUID0),
                             Receiver();
                         exit -> ok
                     end
                 end,
             Setters = [spawn_link(ReceiverFun) || _ <- lists:seq(1, ?SET_SCOPER_WORKERS)],

             Res =
                 try set_scopes6(Entry, NewScopeUUID, Setters, [], 0, ?SET_SCOPE_BATCH_SIZE) of
                     Result -> Result
                 catch
                     _:Reason ->
                         {error, Reason}
                 end,

             [Setter ! exit || Setter <- Setters],
             Res
         end).

%%--------------------------------------------------------------------
%% @doc
%% Internal helper fo set_scopes/2. Dispatch all set_scope jobs across all worker proceses.
%% @end
%%--------------------------------------------------------------------
-spec set_scopes6(Entry :: entry() | [entry()], NewScopeUUID :: uuid(), [pid()], [pid()],
    Offset :: non_neg_integer(), BatchSize :: non_neg_integer()) -> ok | no_return().
set_scopes6(Entry, NewScopeUUID, [], SettersBak, Offset, BatchSize) -> %% Empty workers list -> restore from busy workers list
    set_scopes6(Entry, NewScopeUUID, SettersBak, [], Offset, BatchSize);
set_scopes6([], _NewScopeUUID, _Setters, _SettersBak, _Offset, _BatchSize) ->
    ok; %% Nothing to do
set_scopes6([Entry | R], NewScopeUUID, [Setter | Setters], SettersBak, Offset, BatchSize) ->  %% set_scopes for all given entries
    ok = set_scopes6(Entry, NewScopeUUID, [Setter | Setters], SettersBak, Offset, BatchSize), %% set_scopes for current entry
    ok = set_scopes6(R, NewScopeUUID, Setters, [Setter | SettersBak], Offset, BatchSize);     %% set_scopes for other entries
set_scopes6(Entry, NewScopeUUID, [Setter | Setters], SettersBak, Offset, BatchSize) -> %% set_scopes for current entry
    Setter ! {Entry, NewScopeUUID}, %% Send job to first available process
    {ok, ChildLinks} = list_children(Entry, Offset, BatchSize), %% Apply this fuction for all children
    case length(ChildLinks) < BatchSize of
        true -> ok;
        false ->
            ok = set_scopes6(Entry, NewScopeUUID, Setters, [Setter | SettersBak], Offset + BatchSize, BatchSize)
    end,
    ok = set_scopes6([{uuid, UUID} || #child_link{uuid = UUID} <- ChildLinks], NewScopeUUID, Setters, [Setter | SettersBak], 0, BatchSize).

%%--------------------------------------------------------------------
%% @doc
%% Internal helper for gen_path/1. Accumulates all intermediate documents and concatenates them into path().
%% @end
%%--------------------------------------------------------------------
-spec gen_path2(entry(), [datastore:document()]) -> {ok, path()} | datastore:generic_error() | no_return().
gen_path2(Entry, Acc) ->
    {ok, #document{} = Doc} = get(Entry),
    case datastore:fetch_link(?LINK_STORE_LEVEL, Doc, parent) of
        {ok, {?ROOT_DIR_UUID, _}} ->
            Tokens = [Token || #document{value = #file_meta{name = Token}} <- [Doc | Acc]],
            {ok, fslogic_path:join([<<?DIRECTORY_SEPARATOR>> | Tokens])};
        {ok, {ParentUUID, _}} ->
            gen_path2({uuid, ParentUUID}, [Doc | Acc])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns uuid() for given file_meta:entry(). Providers for example path() -> uuid() conversion.
%% @end
%%--------------------------------------------------------------------
-spec to_uuid(entry()) -> {ok, uuid()} | datastore:generic_error().
to_uuid({uuid, UUID}) ->
    {ok, UUID};
to_uuid(#document{key = UUID}) ->
    {ok, UUID};
to_uuid({path, Path}) ->
    ?run(begin
             {ok, {Doc, _}} = resolve_path(Path),
             to_uuid(Doc)
         end).

%%--------------------------------------------------------------------
%% @doc
%% Check if given term is valid path()
%% @end
%%--------------------------------------------------------------------
-spec is_valid_filename(term()) -> boolean().
is_valid_filename(<<"">>) ->
    false;
is_valid_filename(FileName) when not is_binary(FileName) ->
    false;
is_valid_filename(FileName) when is_binary(FileName) ->
    case binary:matches(FileName, <<?DIRECTORY_SEPARATOR>>) of
        [] -> true;
        _ -> false
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns just error reason for given error tuple
%% @end
%%--------------------------------------------------------------------
-spec normalize_error(term()) -> term().
normalize_error({badmatch, Reason}) ->
    normalize_error(Reason);
normalize_error({case_clause, Reason}) ->
    normalize_error(Reason);
normalize_error({error, Reason}) ->
    normalize_error(Reason);
normalize_error({ok, Inv}) ->
    normalize_error({invalid_response, normalize_error(Inv)});
normalize_error(Reason) ->
    Reason.