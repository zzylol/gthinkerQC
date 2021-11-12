#ifndef COMBINER_H
#define COMBINER_H

#ifdef SECONDARY_KEY_MSG
#include "serialization.h"
template <class SecondaryKey, class MVal>
struct Msg2ndKey
{
	typedef SecondaryKey SecondaryKeyT;
	typedef MVal MValT;

	SecondaryKey key;
	MVal val;

	friend ibinstream& operator<<(ibinstream& m, const Msg2ndKey<SecondaryKey, MVal>& v)
	{
		m << v.key;
		m << v.val;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, Msg2ndKey<SecondaryKey, MVal>& v)
	{
		m >> v.key;
		m >> v.val;
		return m;
	}
};

//-------------------------------------

template <class Msg2ndKeyT>
class Combiner
{
	public:
		typedef typename Msg2ndKeyT::MValT MVal;
		virtual void combine(MVal& old, const MVal& new_msg) = 0;
};

#else

template <class MessageT>
class Combiner {

public:
    virtual void combine(MessageT& old, const MessageT& new_msg) = 0;

};

#endif

#endif
