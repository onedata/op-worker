%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for management of file_meta representation
%%% of posix links.
%%% Note: in posix link is equal to hardlink.
%%% @end
%%%-------------------------------------------------------------------
-module(file_meta_hardlinks).
-author("Michal Wrzeszcz").

-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/posix/errno.hrl").

%% API
-export([empty_references/0, new_doc/5, merge_link_and_file_doc/2,
    register/2, deregister/2,
    count_references/1, inspect_references/1, list_references/1,
    merge_references/2, update_stats_on_merge/2]).

-type link() :: file_meta:uuid().
% List of links to file. It is kept as a map where list of links is divided
% into lists of links created by providers. Such structure is needed for
% conflicts resolution (see merge_references/2).
-type references() :: #{oneprovider:id() => [link()]}.
-type references_list() :: [link()].
-type references_presence() :: no_references_left | has_at_least_one_reference.
-export_type([link/0, references/0, references_list/0, references_presence/0]).

-define(MAX_LINKS_NUM, 1000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes empty references structure stored in #file_meta{} record.
%% @end
%%--------------------------------------------------------------------
-spec empty_references() -> references().
empty_references() ->
    #{}.

%%--------------------------------------------------------------------
%% @doc
%% Creates document for link.
%% NOTE: when link document is requested it is merged using document representing
%% link and document representing target file (see file_meta:get_including_deleted/1).
%% @end
%%--------------------------------------------------------------------
-spec new_doc(file_meta:uuid(), file_meta:name(), file_meta:uuid(), od_space:id(), boolean()) -> file_meta:doc().
new_doc(FileUuid, FileName, ParentUuid, SpaceId, IgnoreInChanges) ->
    #document{
        key = fslogic_file_id:gen_link_uuid(FileUuid),
        value = #file_meta{
            name = FileName,
            type = ?LINK_TYPE,
            parent_uuid = ParentUuid,
            provider_id = oneprovider:get_id()
        },
        scope = SpaceId,
        ignore_in_changes = IgnoreInChanges
    }.

-spec merge_link_and_file_doc(file_meta:doc(), file_meta:doc()) -> {ok, file_meta:doc()}.
merge_link_and_file_doc(LinkDoc = #document{value = LinkRecord}, #document{value = FileRecord}) ->
    {ok, LinkDoc#document{
        value = LinkRecord#file_meta{
            mode = FileRecord#file_meta.mode,
            acl = FileRecord#file_meta.acl,
            owner = FileRecord#file_meta.owner,
            references = undefined
        }
    }}.

-spec register(file_meta:uuid(), link()) -> {ok, file_meta:doc()} | {error, term()}.
register(FileUuid, LinkUuid) ->
    ProviderId = oneprovider:get_id(),
    file_meta:update_including_deleted(FileUuid, fun(#file_meta{references = References} = Record) ->
        case count_references_in_map(References) of
            LinksNum when LinksNum >= ?MAX_LINKS_NUM ->
                {error, ?EMLINK};
            _ ->
                ProviderReferences = maps:get(ProviderId, References, []),
                {ok, Record#file_meta{references = References#{ProviderId => [LinkUuid | ProviderReferences]}}}
        end
    end).

-spec deregister(file_meta:uuid(), link()) -> {ok, references_presence()} | {error, term()}.
deregister(FileUuid, LinkUuid) ->
    ProviderId = oneprovider:get_id(),
    UpdateAns = file_meta:update_including_deleted(FileUuid, fun(#file_meta{references = References} = Record) ->
        ProviderReferences = maps:get(ProviderId, References, []),
        case ProviderReferences -- [LinkUuid] of
            [] -> {ok, Record#file_meta{references = maps:remove(ProviderId, References)}};
            NewProviderReferences -> {ok, Record#file_meta{references = References#{ProviderId => NewProviderReferences}}}
        end
    end),

    case UpdateAns of
        {ok, Doc} -> {ok, inspect_references(Doc)};
        Other -> Other
    end.

-spec count_references(file_meta:uuid() | file_meta:doc()) -> {ok, non_neg_integer()} | {error, term()}.
count_references(#document{key = Key, value = #file_meta{references = undefined}}) ->
    count_references(Key);
count_references(Doc = #document{value = #file_meta{references = References}}) ->
    ReferencesCount = count_references_in_map(References),
    case file_meta:is_deleted(Doc) of
        true -> {ok, ReferencesCount};
        false -> {ok, ReferencesCount + 1}
    end;
count_references(Key) ->
    case file_meta:get_including_deleted(fslogic_file_id:ensure_referenced_uuid(Key)) of
        {ok, Doc} -> count_references(Doc);
        Other -> Other
    end.

-spec inspect_references(file_meta:uuid() | file_meta:doc()) -> references_presence().
inspect_references(KeyOrDoc) ->
    case count_references(KeyOrDoc) of
        {ok, 0} -> no_references_left;
        {ok, _} -> has_at_least_one_reference
    end.

-spec list_references(file_meta:uuid() | file_meta:doc()) -> {ok, [link()]} | {error, term()}.
list_references(#document{key = Key, value = #file_meta{references = undefined}}) ->
    list_references(Key);
list_references(Doc = #document{key = TargetKey, value = #file_meta{references = References}}) ->
    ReferencesList = references_to_list(References),
    case file_meta:is_deleted(Doc) of
        true -> {ok, ReferencesList};
        %% @TODO VFS-7555 Allow for listing file references using any file doc
        false -> {ok, [TargetKey | ReferencesList]}  
    end;
list_references(Key) ->
    case file_meta:get_including_deleted(fslogic_file_id:ensure_referenced_uuid(Key)) of
        {ok, Doc} -> list_references(Doc);
        Other -> Other
    end.

-spec merge_references(file_meta:doc(), file_meta:doc()) -> not_mutated | {mutated, references()}.
merge_references(#document{mutators = [Mutator | _], value = #file_meta{references = NewReferences}},
    #document{value = #file_meta{references = OldReferences}}) ->
    ChangedMutatorReferences = maps:get(Mutator, NewReferences, []),
    OldMutatorReferences = maps:get(Mutator, OldReferences, []),

    case ChangedMutatorReferences of
        OldMutatorReferences -> not_mutated;
        _ -> {mutated, OldReferences#{Mutator => ChangedMutatorReferences}}
    end.


-spec update_stats_on_merge(file_meta:doc(), file_meta:doc()) -> ok.
update_stats_on_merge(
    #document{mutators = [Mutator | _], value = #file_meta{references = NewReferences}} = NewDoc,
    #document{key = Uuid, scope = SpaceId, value = #file_meta{references = OldReferences}} = OldDoc
) ->
    ChangedMutatorReferences = filter_special_references(maps:get(Mutator, NewReferences, [])),
    OldMutatorReferences = maps:get(Mutator, OldReferences, []),

    IsDeleted = file_meta:is_deleted(NewDoc) andalso not file_meta:is_deleted(OldDoc),
    case ChangedMutatorReferences of
        OldMutatorReferences when IsDeleted ->
            dir_size_stats:handle_references_list_changes(file_id:pack_guid(Uuid, SpaceId), [], [Uuid], [Uuid]);
        OldMutatorReferences ->
            ok;
        _ ->
            {ok, OldRefsList} = list_references(OldDoc),
            RemovedReferences = case IsDeleted of
                true -> [Uuid] ++ OldMutatorReferences -- ChangedMutatorReferences;
                false -> OldMutatorReferences -- ChangedMutatorReferences
            end,
            dir_size_stats:handle_references_list_changes(
                file_id:pack_guid(Uuid, SpaceId),
                ChangedMutatorReferences -- OldMutatorReferences,
                RemovedReferences,
                OldRefsList
            )
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec references_to_list(references()) -> references_list().
references_to_list(References) ->
    % Note - do not use lists:flatten as it traverses sublists and it is not necessary here
    % Note - it is expected that function returns references in the same order any time its called - if
    %        new reference is added, the order of the others should remain unchanged
    ProviderId = oneprovider:get_id(),
    lists:flatmap(fun
        (Key) when Key =:= ProviderId -> maps:get(Key, References);
        (Key) -> filter_special_references(maps:get(Key, References))
    end, lists:sort(maps:keys(References))).


-spec count_references_in_map(references()) -> non_neg_integer().
count_references_in_map(References) ->
    ProviderId = oneprovider:get_id(),
    maps:fold(fun
        (Key, ProviderReferences, Acc) when Key =:= ProviderId -> length(ProviderReferences) + Acc;
        (_, ProviderReferences, Acc) -> length(filter_special_references(ProviderReferences)) + Acc
    end, 0, References).


-spec filter_special_references(references_list()) -> references_list().
filter_special_references(References) ->
    lists:filter(fun
        (?OPENED_DELETED_FILE_LINK_PATTERN) -> false;
        (_) -> true
    end, References).
