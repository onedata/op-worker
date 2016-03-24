/**
 * @file rt_local_term.h
 * @author Konrad Zemek
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_LOCAL_TERM_H
#define RT_LOCAL_TERM_H

#include "../nifpp.h"

#include <memory>

namespace one {
namespace provider {

/**
 * The rt_local_term class.
 * rt_local_term object wraps Erlang term by copying it to new environment
 */
class rt_local_term {
    struct shared_data {
        shared_data()
            : env_{enif_alloc_env()}
            , term_{enif_make_atom(env_, "undefined")}
        {
        }

        shared_data(nifpp::TERM src_term)
            : env_{enif_alloc_env()}
            , term_{enif_make_copy(env_, src_term)}
        {
        }

        ~shared_data() { enif_free_env(env_); }

        ErlNifEnv *env_;
        ERL_NIF_TERM term_;
    };

public:
    /**
     * rt_local_term constructor.
     * Constructs wrapper for Erlang atom 'undefined'.
     */
    rt_local_term()
        : shared_data_{std::make_shared<shared_data>()}
    {
    }

    /**
     * rt_local_term constructor.
     * Constructs Erlang term wrapper.
     * @param src_term term to be wrapped
     */
    rt_local_term(nifpp::TERM src_term)
        : shared_data_{std::make_shared<shared_data>(src_term)}
    {
    }

    /// Getter for wrapped Erlang term
    nifpp::TERM get(ErlNifEnv *dst_env) const
    {
        return nifpp::TERM{enif_make_copy(dst_env, shared_data_->term_)};
    }

    /**
     * Compares this rt_local_term with other rt_local_term.
     * @param term to be compared with
     * @return true if wrapped Erlang term is equal to other wrapped Erlang term
     */
    bool operator==(const rt_local_term &rhs) const
    {
        return shared_data_ == rhs.shared_data_ ||
               enif_compare(shared_data_->term_, rhs.shared_data_->term_) == 0;
    }

    /**
     * Compares this rt_local_term with other rt_local_term.
     * @param term to be compared with
     * @return true if wrapped Erlang term is less than other wrapped Erlang
     * term
     */
    bool operator<(const rt_local_term &rhs) const
    {
        return shared_data_ != rhs.shared_data_ &&
               enif_compare(shared_data_->term_, rhs.shared_data_->term_) < 0;
    }

private:
    std::shared_ptr<const shared_data> shared_data_;
    friend struct std::hash<rt_local_term>;
};

} // namespace provider
} // namespace one

#endif // RT_LOCAL_TERM_H
