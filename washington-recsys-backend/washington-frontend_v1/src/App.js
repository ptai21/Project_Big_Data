import React, { useState, useEffect } from 'react';
import { Search, MapPin, Star, ChevronDown, ChevronLeft, ChevronRight, User, Calendar } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend } from 'recharts';

const API_BASE = 'http://127.0.0.1:8000/api/v1';

const getAddressFromCoords = async (lat, lng) => {
  try {
    const response = await fetch(
      `https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&format=json`
    );
    const data = await response.json();
    return data.display_name || null;
  } catch (e) {
    return null;
  }
};

const Select = ({ label, value, onChange, options, placeholder }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState('');
  
  const filtered = options.filter(opt => 
    opt.toLowerCase().includes(search.toLowerCase())
  );
  
  return (
    <div className="mb-3">
      <label className="block text-xs font-medium text-gray-700 mb-1">{label}</label>
      <div className="relative">
        <div 
          className="w-full border border-gray-300 rounded px-2 py-1.5 bg-white cursor-pointer flex items-center justify-between hover:border-orange-400 transition-colors text-sm"
          onClick={() => setIsOpen(!isOpen)}
        >
          <span className={value ? 'text-gray-900' : 'text-gray-400'}>{value || placeholder}</span>
          <ChevronDown className={`w-3 h-3 text-gray-400 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
        </div>
        
        {isOpen && (
          <div className="absolute z-50 w-full mt-1 bg-white border border-gray-200 rounded shadow-lg overflow-hidden">
            <div className="p-1.5 border-b">
              <input
                type="text"
                placeholder="Search..."
                className="w-full border border-gray-200 rounded px-2 py-1 text-xs focus:outline-none focus:border-orange-400"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                onClick={(e) => e.stopPropagation()}
              />
            </div>
            <div className="max-h-40 overflow-y-auto">
              <div 
                className="px-2 py-1.5 hover:bg-orange-50 cursor-pointer text-gray-500 text-xs"
                onClick={() => { onChange(''); setIsOpen(false); setSearch(''); }}
              >
                All
              </div>
              {filtered.map(opt => (
                <div 
                  key={opt}
                  className="px-2 py-1.5 hover:bg-orange-50 cursor-pointer text-gray-700 text-xs"
                  onClick={() => { onChange(opt); setIsOpen(false); setSearch(''); }}
                >
                  {opt}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

const BusinessCard = ({ business, onClick, isSelected, compact = false }) => {
  const [address, setAddress] = useState('Loading...');

  useEffect(() => {
    const fetchAddress = async () => {
      if (business.address && business.address !== 'null' && business.address !== null) {
        setAddress(business.address);
        return;
      }
      
      if (business.latitude && business.longitude) {
        const addr = await getAddressFromCoords(business.latitude, business.longitude);
        if (addr) {
          setAddress(addr);
          return;
        }
      }
      
      const parts = [];
      if (business.city && business.city !== 'null') parts.push(business.city);
      if (business.county && business.county !== 'null') parts.push(business.county);
      setAddress(parts.length > 0 ? parts.join(', ') : 'No address available');
    };

    fetchAddress();
  }, [business]);

  return (
    <div 
      className={`border rounded-lg p-3 cursor-pointer transition-all ${
        isSelected 
          ? 'border-orange-400 bg-orange-50' 
          : 'border-gray-200 bg-white hover:border-orange-300 hover:shadow-md'
      }`}
      onClick={onClick}
    >
      <div className="flex justify-between items-start mb-1">
        <h3 className="font-semibold text-gray-900 text-sm line-clamp-1 flex-1">{business.name}</h3>
        {!compact && <span className="text-xs text-orange-500 ml-2">Read more</span>}
      </div>
      
      <div className="flex items-center gap-1 mb-1">
        <div className="flex">
          {[1,2,3,4,5].map(i => (
            <Star 
              key={i} 
              className={`w-3 h-3 ${i <= Math.round(business.avg_rating || 0) ? 'text-orange-400 fill-orange-400' : 'text-gray-300'}`} 
            />
          ))}
        </div>
        <span className="text-xs text-gray-600 ml-1">
          {business.avg_rating?.toFixed(1) || 'N/A'} ({business.num_of_reviews?.toLocaleString() || 0})
        </span>
      </div>
      
      <p className="text-xs text-gray-500 mb-1 line-clamp-1">
        {business.original_category 
          ? (business.original_category.length > 40 
              ? business.original_category.substring(0, 40) + '...' 
              : business.original_category)
          : 'Uncategorized'}
      </p>
      
      <div className="flex items-start gap-1 text-xs text-gray-500">
        <MapPin className="w-3 h-3 mt-0.5 flex-shrink-0" />
        <span className="line-clamp-1">{address}</span>
      </div>
    </div>
  );
};

const RatingBar = ({ rating, count, maxCount }) => {
  const percentage = maxCount ? (count / maxCount) * 100 : 0;
  return (
    <div className="flex items-center gap-2">
      <span className="w-3 text-xs text-gray-600">{rating}</span>
      <div className="flex-1 h-2 bg-gray-100 rounded overflow-hidden">
        <div 
          className="h-full bg-orange-400 rounded transition-all duration-500"
          style={{ width: `${percentage}%` }}
        />
      </div>
      <span className="text-xs text-gray-500 w-12 text-right">({count?.toLocaleString() || 0})</span>
    </div>
  );
};

const ReviewCard = ({ review }) => {
  const formatDate = (dateStr) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', { day: '2-digit', month: '2-digit', year: 'numeric' });
  };

  return (
    <div className="border border-gray-200 rounded-lg p-3 bg-white">
      <div className="flex justify-between items-start mb-2">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 bg-gray-200 rounded-full flex items-center justify-center">
            <User className="w-4 h-4 text-gray-500" />
          </div>
          <div>
            <p className="text-sm font-medium text-gray-900">Customer</p>
            <div className="flex items-center gap-1">
              {[1,2,3,4,5].map(i => (
                <Star 
                  key={i} 
                  className={`w-3 h-3 ${i <= review.rating ? 'text-orange-400 fill-orange-400' : 'text-gray-300'}`} 
                />
              ))}
            </div>
          </div>
        </div>
        <div className="flex items-center gap-1 text-xs text-gray-500">
          <Calendar className="w-3 h-3" />
          {formatDate(review.time)}
        </div>
      </div>
      <p className="text-sm text-gray-600 line-clamp-3">{review.text}</p>
      <div className="mt-2 flex items-center gap-2">
        <span className={`text-xs px-2 py-0.5 rounded ${
          review.sentiment_label === 'positive' ? 'bg-green-100 text-green-700' :
          review.sentiment_label === 'negative' ? 'bg-red-100 text-red-700' :
          'bg-gray-100 text-gray-700'
        }`}>
          {review.sentiment_label}
        </span>
      </div>
    </div>
  );
};

const CustomTooltip = ({ active, payload, isYearly }) => {
  if (active && payload && payload.length) {
    const data = payload[0].payload;
    return (
      <div className="bg-white border border-gray-200 rounded-lg p-2 shadow-lg text-xs">
        <p className="font-semibold text-gray-900">
          {isYearly ? data.year : `${data.month}/${data.year}`}
        </p>
        {payload.map((p, i) => (
          <p key={i} style={{ color: p.color }}>
            {p.name}: {typeof p.value === 'number' ? (p.name.includes('Sentiment') ? `${(p.value * 100).toFixed(1)}%` : p.value) : p.value}
          </p>
        ))}
      </div>
    );
  }
  return null;
};

// Page 3: Reviews Page
const ReviewsPage = ({ business, onBack }) => {
  const [reviews, setReviews] = useState([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const [filter, setFilter] = useState('all');
  
  useEffect(() => {
    if (business) {
      setLoading(true);
      let url = `${API_BASE}/businesses/${business.business_id}/reviews?page=${page}&page_size=10`;
      if (filter !== 'all') {
        url += `&rating=${filter}`;
      }
      fetch(url)
        .then(r => r.json())
        .then(data => {
          setReviews(data.data || []);
          setTotal(data.total || 0);
          setLoading(false);
        })
        .catch(() => setLoading(false));
    }
  }, [business, page, filter]);
  
  const totalPages = Math.ceil(total / 10);
  
  // Count reviews by rating (mock - would need API)
  const ratingCounts = { 5: 0, 4: 0, 3: 0, 2: 0, 1: 0 };
  
  return (
    <div className="h-full flex flex-col bg-gray-50">
      <div className="border-b border-gray-200 p-4 bg-white">
        <button 
          onClick={onBack}
          className="flex items-center gap-1 text-gray-600 hover:text-orange-500 transition-colors mb-2"
        >
          <ChevronLeft className="w-4 h-4" />
          <span className="text-sm">Back to details</span>
        </button>
        
        <h1 className="text-xl font-bold text-gray-900">{business?.name}</h1>
        <p className="text-sm text-gray-600">All Reviews ({total})</p>
      </div>
      
      {/* Filter tabs */}
      <div className="bg-white border-b border-gray-200 px-4 py-2 flex gap-2 overflow-x-auto">
        <button
          onClick={() => { setFilter('all'); setPage(1); }}
          className={`px-3 py-1 rounded text-sm whitespace-nowrap ${filter === 'all' ? 'bg-orange-500 text-white' : 'bg-gray-100 text-gray-600'}`}
        >
          All
        </button>
        {[5,4,3,2,1].map(rating => (
          <button
            key={rating}
            onClick={() => { setFilter(String(rating)); setPage(1); }}
            className={`px-3 py-1 rounded text-sm whitespace-nowrap ${filter === String(rating) ? 'bg-orange-500 text-white' : 'bg-gray-100 text-gray-600'}`}
          >
            {rating} ⭐
          </button>
        ))}
      </div>
      
      {/* Reviews list */}
      <div className="flex-1 overflow-y-auto p-4 space-y-3">
        {loading ? (
          [...Array(3)].map((_, i) => (
            <div key={i} className="bg-white border border-gray-200 rounded-lg p-3 animate-pulse">
              <div className="h-4 bg-gray-200 rounded w-1/3 mb-2" />
              <div className="h-3 bg-gray-200 rounded w-full mb-1" />
              <div className="h-3 bg-gray-200 rounded w-2/3" />
            </div>
          ))
        ) : reviews.length > 0 ? (
          reviews.map(review => (
            <ReviewCard key={review.review_id} review={review} />
          ))
        ) : (
          <p className="text-center text-gray-500 py-8">No reviews found</p>
        )}
      </div>
      
      {/* Pagination */}
      {totalPages > 1 && (
        <div className="bg-white border-t border-gray-200 p-3 flex justify-center items-center gap-2">
          <button
            onClick={() => setPage(p => Math.max(1, p - 1))}
            disabled={page === 1}
            className="p-1 border border-gray-300 rounded disabled:opacity-50"
          >
            <ChevronLeft className="w-4 h-4" />
          </button>
          <span className="text-sm text-gray-600">
            {page} / {totalPages}
          </span>
          <button
            onClick={() => setPage(p => Math.min(totalPages, p + 1))}
            disabled={page === totalPages}
            className="p-1 border border-gray-300 rounded disabled:opacity-50"
          >
            <ChevronRight className="w-4 h-4" />
          </button>
        </div>
      )}
    </div>
  );
};

// Page 2: Detail View
const DetailView = ({ business, onBack, onViewReviews }) => {
  const [detail, setDetail] = useState(null);
  const [address, setAddress] = useState('Loading...');
  const [reviewSummary, setReviewSummary] = useState(null);
  const [statsTotal, setStatsTotal] = useState(null);
  const [statsYearly, setStatsYearly] = useState([]);
  const [statsMonthly, setStatsMonthly] = useState([]);
  const [recentReviews, setRecentReviews] = useState([]);
  const [selectedPeriod, setSelectedPeriod] = useState('all');
  const [loading, setLoading] = useState(true);
  
  useEffect(() => {
    if (business) {
      setLoading(true);
      setAddress('Loading...');
      
      Promise.all([
        fetch(`${API_BASE}/businesses/${business.business_id}`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/businesses/${business.business_id}/reviews/summary`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/businesses/${business.business_id}/stats/total`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/businesses/${business.business_id}/stats/yearly`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/businesses/${business.business_id}/stats/monthly`).then(r => r.json()).catch(() => null),
        fetch(`${API_BASE}/businesses/${business.business_id}/reviews?page=1&page_size=2`).then(r => r.json()).catch(() => null),
      ]).then(async ([detailData, review, total, yearly, monthly, reviews]) => {
        setDetail(detailData);
        setReviewSummary(review);
        setStatsTotal(total);
        setRecentReviews(reviews?.data || []);
        
        if (detailData) {
          if (detailData.address && detailData.address !== 'null' && detailData.address !== null) {
            setAddress(detailData.address);
          } else if (detailData.latitude && detailData.longitude) {
            const addr = await getAddressFromCoords(detailData.latitude, detailData.longitude);
            if (addr) {
              setAddress(addr);
            } else {
              const parts = [];
              if (detailData.city && detailData.city !== 'null') parts.push(detailData.city);
              if (detailData.county && detailData.county !== 'null') parts.push(detailData.county);
              setAddress(parts.length > 0 ? parts.join(', ') : 'No address available');
            }
          } else {
            const parts = [];
            if (detailData.city && detailData.city !== 'null') parts.push(detailData.city);
            if (detailData.county && detailData.county !== 'null') parts.push(detailData.county);
            setAddress(parts.length > 0 ? parts.join(', ') : 'No address available');
          }
        }
        
        const yearlyData = (yearly?.data || [])
          .sort((a, b) => a.year - b.year)
          .map(item => ({
            ...item,
            avg_sentiment: parseFloat(item.avg_sentiment) || 0
          }));
        setStatsYearly(yearlyData);
        
        const monthlyData = (monthly?.data || [])
          .sort((a, b) => (a.year * 12 + a.month) - (b.year * 12 + b.month))
          .map(item => ({
            ...item,
            avg_sentiment: parseFloat(item.avg_sentiment) || 0,
            label: `${item.month}/${item.year}`
          }));
        setStatsMonthly(monthlyData);
        
        setLoading(false);
      });
    }
  }, [business]);
  
  if (!business) return null;
  
  const displayData = detail || business;
  
  const maxReviewCount = reviewSummary?.rating_distribution 
    ? Math.max(...reviewSummary.rating_distribution.map(r => r.count))
    : 0;
  
  let hours = null;
  try {
    if (displayData.hours && displayData.hours !== 'null' && displayData.hours !== null) {
      hours = JSON.parse(displayData.hours);
    }
  } catch (e) {
    hours = null;
  }

  const dayOrder = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];
  
  const sortedHours = hours ? dayOrder
    .filter(day => hours[day] !== undefined)
    .map(day => ({ day, time: hours[day] })) : [];

  const getDescription = () => {
    if (displayData.description && displayData.description !== 'null' && displayData.description !== null) {
      return displayData.description;
    }
    return 'No description available';
  };

  // Get chart data based on selected period
  const getChartData = () => {
    if (selectedPeriod === 'all') {
      return statsYearly;
    } else {
      return statsMonthly.filter(item => item.year === parseInt(selectedPeriod));
    }
  };

  const chartData = getChartData();
  const availableYears = [...new Set(statsYearly.map(item => item.year))].sort();
  
  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="border-b border-gray-200 p-4 bg-white">
        <button 
          onClick={onBack}
          className="flex items-center gap-1 text-gray-600 hover:text-orange-500 transition-colors mb-2"
        >
          <ChevronLeft className="w-4 h-4" />
          <span className="text-sm">Back to list</span>
        </button>
        
        <h1 className="text-xl font-bold text-gray-900">{displayData.name}</h1>
        
        <div className="flex items-center gap-1 mt-1">
          <div className="flex">
            {[1,2,3,4,5].map(i => (
              <Star 
                key={i} 
                className={`w-4 h-4 ${i <= Math.round(displayData.avg_rating || 0) ? 'text-orange-400 fill-orange-400' : 'text-gray-300'}`} 
              />
            ))}
          </div>
          <span className="text-sm text-gray-600 ml-1">
            {displayData.avg_rating?.toFixed(1)} ({displayData.num_of_reviews?.toLocaleString()})
          </span>
        </div>
      </div>
      
      {/* Content */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {/* Description */}
        <div>
          <span className="font-semibold text-gray-900">Description: </span>
          <span className="text-gray-600 text-sm">{getDescription()}</span>
        </div>
        
        {/* Category */}
        <div>
          <span className="font-semibold text-gray-900">Category: </span>
          <span className="text-gray-600 text-sm">{displayData.original_category || 'N/A'}</span>
        </div>

        {/* Location */}
        <div>
          <h3 className="font-semibold text-gray-900 mb-2">Location</h3>
          <div className="bg-gray-50 rounded-lg p-3 space-y-2">
            <div className="flex items-start gap-2">
              <MapPin className="w-4 h-4 mt-0.5 text-orange-500 flex-shrink-0" />
              <div className="text-sm">
                <p className="text-gray-900">{address}</p>
                {(displayData.city || displayData.county) && (
                  <p className="text-gray-500 mt-1">
                    {[displayData.city, displayData.county].filter(x => x && x !== 'null').join(', ')}
                  </p>
                )}
              </div>
            </div>
            {displayData.latitude && displayData.longitude && (
              <div className="text-xs text-gray-400">
                Coordinates: {displayData.latitude}, {displayData.longitude}
              </div>
            )}
          </div>
        </div>

        {/* Hours + Review Summary side by side */}
        <div className="grid grid-cols-2 gap-4">
          {/* Hours */}
          <div>
            <h3 className="font-semibold text-gray-900 mb-2">Hours</h3>
            {sortedHours.length > 0 ? (
              <div className="border border-gray-200 rounded-lg overflow-hidden text-xs">
                <table className="w-full">
                  <tbody>
                    {sortedHours.map(({ day, time }, idx) => (
                      <tr key={day} className={idx % 2 === 0 ? 'bg-gray-50' : 'bg-white'}>
                        <td className="px-2 py-1 text-gray-600">{day.slice(0, 3)}</td>
                        <td className={`px-2 py-1 text-right ${time === 'Closed' ? 'text-red-500' : 'text-gray-900'}`}>{time}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="text-sm text-gray-500">No hours available</p>
            )}
          </div>
          
          {/* Review Summary */}
          <div>
            <h3 className="font-semibold text-gray-900 mb-2">Reviews</h3>
            {loading ? (
              <div className="space-y-1">
                {[5,4,3,2,1].map(i => (
                  <div key={i} className="h-2 bg-gray-100 rounded animate-pulse" />
                ))}
              </div>
            ) : (
              <div className="space-y-1">
                {[5,4,3,2,1].map(rating => {
                  const item = reviewSummary?.rating_distribution?.find(r => r.rating === rating);
                  return (
                    <RatingBar 
                      key={rating} 
                      rating={rating} 
                      count={item?.count || 0} 
                      maxCount={maxReviewCount} 
                    />
                  );
                })}
              </div>
            )}
          </div>
        </div>

        {/* Recent Reviews Preview */}
        {recentReviews.length > 0 && (
          <div>
            <div className="flex justify-between items-center mb-2">
              <h3 className="font-semibold text-gray-900">Recent Reviews</h3>
              <button 
                onClick={onViewReviews}
                className="text-sm text-orange-500 hover:text-orange-600"
              >
                View all →
              </button>
            </div>
            <div className="space-y-2">
              {recentReviews.slice(0, 1).map(review => (
                <ReviewCard key={review.review_id} review={review} />
              ))}
            </div>
            <button 
              onClick={onViewReviews}
              className="mt-2 text-sm text-orange-500 hover:text-orange-600 font-medium"
            >
              More
            </button>
          </div>
        )}
        
        {/* Further Analysis */}
        {statsTotal && (
          <div>
            <h3 className="font-semibold text-gray-900 mb-2">Further analysis</h3>
            <div className="flex gap-2">
              <div className="flex-1 bg-green-50 border border-green-200 rounded p-2 text-center">
                <p className="text-lg font-bold text-green-600">{statsTotal.positive_count || 0}</p>
                <p className="text-xs text-green-600">Positive ({statsTotal.positive_pct || 0}%)</p>
              </div>
              <div className="flex-1 bg-gray-50 border border-gray-200 rounded p-2 text-center">
                <p className="text-lg font-bold text-gray-600">{statsTotal.neutral_count || 0}</p>
                <p className="text-xs text-gray-600">Neutral ({statsTotal.neutral_pct || 0}%)</p>
              </div>
              <div className="flex-1 bg-red-50 border border-red-200 rounded p-2 text-center">
                <p className="text-lg font-bold text-red-600">{statsTotal.negative_count || 0}</p>
                <p className="text-xs text-red-600">Negative ({statsTotal.negative_pct || 0}%)</p>
              </div>
            </div>
          </div>
        )}

        {/* Period Selector */}
        {(statsYearly.length > 0 || statsMonthly.length > 0) && (
          <div className="flex justify-end">
            <select
              value={selectedPeriod}
              onChange={(e) => setSelectedPeriod(e.target.value)}
              className="border border-gray-300 rounded px-3 py-1 text-sm focus:outline-none focus:border-orange-400"
            >
              <option value="all">All (Yearly)</option>
              {availableYears.map(year => (
                <option key={year} value={year}>{year} (Monthly)</option>
              ))}
            </select>
          </div>
        )}
        
        {/* Charts side by side */}
        {chartData.length > 0 && (
          <div className="grid grid-cols-2 gap-4">
            {/* Average Sentiment Score */}
            <div>
              <h3 className="font-semibold text-gray-900 mb-2 text-sm">Average sentiment score</h3>
              <div className="border border-gray-200 rounded-lg p-3">
                <div className="h-44">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                      <XAxis 
                        dataKey={selectedPeriod === 'all' ? 'year' : 'month'}
                        tick={{ fontSize: 10, fill: '#6b7280' }} 
                        axisLine={{ stroke: '#e5e7eb' }} 
                        tickLine={false} 
                      />
                      <YAxis 
                        tick={{ fontSize: 10, fill: '#6b7280' }} 
                        axisLine={{ stroke: '#e5e7eb' }} 
                        tickLine={false}
                        domain={[0, 1]}
                        tickFormatter={(value) => `${(value * 100).toFixed(0)}%`}
                      />
                      <Tooltip content={<CustomTooltip isYearly={selectedPeriod === 'all'} />} />
                      <Line 
                        type="monotone" 
                        dataKey="avg_sentiment"
                        name="Avg Sentiment"
                        stroke="#f97316" 
                        strokeWidth={2} 
                        dot={{ fill: '#f97316', strokeWidth: 2, r: 3 }}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>

            {/* Number of each sentiment label */}
            <div>
              <h3 className="font-semibold text-gray-900 mb-2 text-sm">Sentiment labels count</h3>
              <div className="border border-gray-200 rounded-lg p-3">
                <div className="h-44">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                      <XAxis 
                        dataKey={selectedPeriod === 'all' ? 'year' : 'month'}
                        tick={{ fontSize: 10, fill: '#6b7280' }} 
                        axisLine={{ stroke: '#e5e7eb' }} 
                        tickLine={false} 
                      />
                      <YAxis 
                        tick={{ fontSize: 10, fill: '#6b7280' }} 
                        axisLine={{ stroke: '#e5e7eb' }} 
                        tickLine={false}
                      />
                      <Tooltip content={<CustomTooltip isYearly={selectedPeriod === 'all'} />} />
                      <Legend wrapperStyle={{ fontSize: '10px' }} />
                      <Line 
                        type="monotone" 
                        dataKey="positive_count"
                        name="Positive"
                        stroke="#22c55e" 
                        strokeWidth={2} 
                        dot={{ fill: '#22c55e', r: 3 }}
                      />
                      <Line 
                        type="monotone" 
                        dataKey="neutral_count"
                        name="Neutral"
                        stroke="#6b7280" 
                        strokeWidth={2} 
                        dot={{ fill: '#6b7280', r: 3 }}
                      />
                      <Line 
                        type="monotone" 
                        dataKey="negative_count"
                        name="Negative"
                        stroke="#ef4444" 
                        strokeWidth={2} 
                        dot={{ fill: '#ef4444', r: 3 }}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

// Page 1: Main App
export default function App() {
  const [businesses, setBusinesses] = useState([]);
  const [loading, setLoading] = useState(false);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [selectedBusiness, setSelectedBusiness] = useState(null);
  const [currentView, setCurrentView] = useState('list'); // 'list', 'detail', 'reviews'
  
  const [filters, setFilters] = useState({
    field: '',
    county: '',
    city: '',
    min_rating: '',
    max_rating: '',
    search: ''
  });
  
  const [filterOptions, setFilterOptions] = useState({
    fields: [],
    counties: [],
    cities: []
  });
  
  useEffect(() => {
    fetch(`${API_BASE}/filters/options`)
      .then(r => r.json())
      .then(data => setFilterOptions(data))
      .catch(console.error);
  }, []);
  
  useEffect(() => {
    if (filters.county) {
      fetch(`${API_BASE}/filters/cities?county=${encodeURIComponent(filters.county)}`)
        .then(r => r.json())
        .then(cities => setFilterOptions(prev => ({ ...prev, cities })))
        .catch(console.error);
    }
  }, [filters.county]);
  
  useEffect(() => {
    setLoading(true);
    const params = new URLSearchParams({ page, page_size: 20 });
    
    if (filters.field) params.append('field', filters.field);
    if (filters.county) params.append('county', filters.county);
    if (filters.city) params.append('city', filters.city);
    if (filters.min_rating) params.append('min_rating', filters.min_rating);
    if (filters.max_rating) params.append('max_rating', filters.max_rating);
    if (filters.search) params.append('search', filters.search);
    
    fetch(`${API_BASE}/businesses?${params}`)
      .then(r => r.json())
      .then(data => {
        setBusinesses(data.data || []);
        setTotal(data.total || 0);
        setLoading(false);
      })
      .catch(err => {
        console.error(err);
        setLoading(false);
      });
  }, [filters, page]);
  
  const handleSelectBusiness = (business) => {
    setSelectedBusiness(business);
    setCurrentView('detail');
  };
  
  const totalPages = Math.ceil(total / 20);
  
  // Render based on current view
  if (currentView === 'reviews' && selectedBusiness) {
    return (
      <div className="min-h-screen bg-gray-50">
        <header className="bg-white border-b border-gray-200 px-6 py-3">
          <h1 className="text-lg font-bold text-gray-900">
            Destination Recommendation System for Washington State
          </h1>
        </header>
        <div className="h-[calc(100vh-52px)]">
          <ReviewsPage 
            business={selectedBusiness} 
            onBack={() => setCurrentView('detail')} 
          />
        </div>
      </div>
    );
  }
  
  if (currentView === 'detail' && selectedBusiness) {
    return (
      <div className="min-h-screen bg-gray-50">
        <header className="bg-white border-b border-gray-200 px-6 py-3">
          <h1 className="text-lg font-bold text-gray-900">
            Destination Recommendation System for Washington State
          </h1>
        </header>
        <div className="h-[calc(100vh-52px)]">
          <DetailView 
            business={selectedBusiness} 
            onBack={() => setCurrentView('list')}
            onViewReviews={() => setCurrentView('reviews')}
          />
        </div>
      </div>
    );
  }
  
  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white border-b border-gray-200 px-6 py-3">
        <h1 className="text-lg font-bold text-gray-900">
          Destination Recommendation System for Washington State
        </h1>
      </header>
      
      <div className="flex h-[calc(100vh-52px)]">
        {/* Filters - Left sidebar */}
        <div className="w-64 bg-white border-r border-gray-200 p-4 flex-shrink-0 overflow-y-auto">
          <Select
            label="Field"
            value={filters.field}
            onChange={v => setFilters(prev => ({ ...prev, field: v }))}
            options={filterOptions.fields || []}
            placeholder="All"
          />
          
          <Select
            label="County"
            value={filters.county}
            onChange={v => setFilters(prev => ({ ...prev, county: v, city: '' }))}
            options={filterOptions.counties || []}
            placeholder="All"
          />
          
          <Select
            label="City"
            value={filters.city}
            onChange={v => setFilters(prev => ({ ...prev, city: v }))}
            options={filterOptions.cities || []}
            placeholder="All"
          />
          
          <Select
            label="Min Rating"
            value={filters.min_rating}
            onChange={v => setFilters(prev => ({ ...prev, min_rating: v }))}
            options={['1', '2', '3', '4', '5']}
            placeholder="All"
          />
          
          <Select
            label="Max Rating"
            value={filters.max_rating}
            onChange={v => setFilters(prev => ({ ...prev, max_rating: v }))}
            options={['1', '2', '3', '4', '5']}
            placeholder="All"
          />
          
          <button 
            className="w-full bg-orange-500 text-white py-1.5 rounded text-sm hover:bg-orange-600 transition-colors font-medium"
            onClick={() => setPage(1)}
          >
            Select
          </button>
        </div>
        
        {/* Business List */}
        <div className="flex-1 bg-white flex flex-col">
          <div className="p-4 border-b border-gray-200">
            <div className="flex items-center justify-between mb-3">
              <p className="text-sm text-gray-600">
                {loading ? 'Loading...' : `${total.toLocaleString()} results found`}
              </p>
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                <input
                  type="text"
                  placeholder="Search by name..."
                  className="pl-9 pr-4 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:border-orange-400"
                  value={filters.search}
                  onChange={e => setFilters(prev => ({ ...prev, search: e.target.value }))}
                />
              </div>
            </div>
          </div>
          
          <div className="flex-1 overflow-y-auto p-4">
            {loading ? (
              <div className="space-y-3">
                {[...Array(5)].map((_, i) => (
                  <div key={i} className="bg-white border border-gray-200 rounded-lg p-4 animate-pulse">
                    <div className="h-5 bg-gray-200 rounded w-1/2 mb-2" />
                    <div className="h-4 bg-gray-200 rounded w-1/3 mb-2" />
                    <div className="h-4 bg-gray-200 rounded w-3/4" />
                  </div>
                ))}
              </div>
            ) : businesses.length > 0 ? (
              <div className="space-y-3">
                {businesses.map(business => (
                  <BusinessCard 
                    key={business.business_id} 
                    business={business} 
                    onClick={() => handleSelectBusiness(business)}
                    isSelected={selectedBusiness?.business_id === business.business_id}
                  />
                ))}
              </div>
            ) : (
              <div className="text-center py-12">
                <MapPin className="w-12 h-12 text-gray-300 mx-auto mb-3" />
                <p className="text-gray-500">No results found</p>
              </div>
            )}
          </div>
          
          {totalPages > 1 && (
            <div className="p-4 border-t border-gray-200 flex justify-center items-center gap-2">
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page === 1}
                className="px-4 py-2 border border-gray-300 rounded-lg text-sm disabled:opacity-50 hover:bg-gray-50"
              >
                Previous
              </button>
              <span className="px-4 py-2 text-sm text-gray-600">
                Page {page} / {totalPages}
              </span>
              <button
                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                disabled={page === totalPages}
                className="px-4 py-2 border border-gray-300 rounded-lg text-sm disabled:opacity-50 hover:bg-gray-50"
              >
                Next
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}