%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: Common defines for dao module
%% @end
%% ===================================================================

-ifndef(COMMON_HRL).
-define(COMMON_HRL, 1).

-define(DAO_REQUEST_TIMEOUT, 5000).

%% View definitions location
-define(VIEW_DEF_LOCATION, "views/").
-define(MAP_DEF_SUFFIX, "_map.js").
-define(REDUCE_DEF_SUFFIX, "_reduce.js").

%% Macros

%% Seeds pseudo-random number generator with current time and hashed node name. <br/>
%% See {@link random:seed/3} and {@link erlang:now/0} for more details
-define(SEED, begin
                IsSeeded = get(proc_seeded),
                if
                    IsSeeded =/= true ->
                        put(proc_seeded, true),
                        {A_SEED, B_SEED, C_SEED} = now(),
                        L_SEED = atom_to_list(node()),
                        {_, Sum_SEED} =  lists:foldl(fun(Elem_SEED, {N_SEED, Acc_SEED}) ->
                            {N_SEED * 137, Acc_SEED + Elem_SEED * N_SEED} end, {1, 0}, L_SEED),
                        random:seed(Sum_SEED*10000 + A_SEED, B_SEED, C_SEED);
                    true -> already_seeded
                end
             end).

%% Returns random positive number from range 1 .. N. This macro is simply shortcut to random:uniform(N)
-define(RND(N), random:uniform(N)).

%% record definition used in record registration example
-record(some_record, {field1 = "", field2 = "", field3 = ""}).

%% Helper macro. See macro ?dao_record_info/1 for more details.
-define(record_info_gen(X), {record_info(size, X), record_info(fields, X), #X{}}).

%% Record-wrapper for regular records that needs to be saved in DB. Adds UUID and Revision info to each record.
%% `uuid` is document UUID, `rev_info` is documents' current revision number
%% `record` is an record representing this document (its data), `force_update` is a flag
%% that forces dao:save_record/1 to update this document even if rev_info isn't valid or up to date.
-record(veil_document, {uuid = "", rev_info = 0, record = none, force_update = false}).

%% Those records represent view result Each #view_resault contains list of #view_row.
%% If the view has been queried with `include_docs` option, #view_row.doc will contain #veil_document, therefore
%% #view_row.id == #view_row.doc#veil_document.uuid. Unfortunately wrapping demanded record in #veil_document is
%% necessary because we may need revision info for that document.
-record(view_row, {id = "", key = "", value = 0, doc = none}).
-record(view_result, {total = 0, offset = 0, rows = []}).

%% These records allows representing databases, design documents and their views.
%% Used in DAO initial configuration in order to easily setup/update views in database.
-record(db_info, {name = "", designs = []}).
-record(design_info, {name = "", version = 0, views = []}).
-record(view_info, {name = "", design = "", db_name = "", version = 0}).

-endif.