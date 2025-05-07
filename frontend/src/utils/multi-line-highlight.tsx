import Fuse from "fuse.js";
import { DocumentColorEnum, highlightColors } from "./colors";

interface WordData {
  text: string;
  spanIdx: number;
  wordIdx: number;
}

export const multiHighlight = (
  textToHighlight: string,
  pageNumber: number,
  color = DocumentColorEnum.yellow
) => {
  console.log("Highlighting text:", textToHighlight.substring(0, 30), "on page:", pageNumber);
  const highlightColor = highlightColors[color];

  // Select spans with better specificity
  const spans = document.querySelectorAll(
    `div[data-page-number='${pageNumber + 1}'] .react-pdf__Page__textContent.textLayer span`
  );
  
  if (spans.length === 0) {
    console.warn("No spans found for highlighting - page may not be fully rendered");
    return false;
  }

  // Extract words from spans
  const words: WordData[] = [];
  spans.forEach((span, spanIdx) => {
    const htmlSpan = span as HTMLElement;
    const spanWords = htmlSpan.textContent || "";
    spanWords.split(" ")
      .filter(word => word.trim().length > 0)
      .forEach((text, wordIdx) => {
        words.push({ text, spanIdx, wordIdx });
      });
  });

  // Normalize search string
  let searchString = textToHighlight.replace(/\s{2,}/g, " ");
  searchString = searchString.replace(/\t/g, " ");
  searchString = searchString.replace(/(\r\n|\n|\r)/g, " ");
  searchString = searchString.trim();

  const searchWords = searchString.split(" ").filter(word => word.trim().length > 0);
  const lenSearchString = searchWords.length;

  if (!lenSearchString) {
    console.warn("Empty search string after normalization");
    return false;
  }

  const firstWord = searchWords[0];
  if (!firstWord) return false;

  // Generate search data
  const searchData = generateDirectSearchData(firstWord, words, lenSearchString);

  // Configure fuzzy search
  const options = {
    includeScore: true,
    threshold: 0.3, // More tolerant matching
    minMatchCharLength: 5,
    shouldSort: true,
    findAllMatches: true,
    includeMatches: true,
    keys: ["text"],
  };

  const fuse = new Fuse(searchData, options);
  const result = fuse.search(searchString);

  if (result.length > 0) {
    const searchResult = result[0]?.item;
    const startSpan = searchResult?.startSpan || 0;
    const endSpan = searchResult?.endSpan || 0;
    const startWordIdx = searchResult?.startWordIdx || 0;
    const endWordIdx = searchResult?.endWordIdx || 0;

    // Apply highlights
    for (let i = startSpan; i <= endSpan; i++) {
      if (i >= spans.length) continue;
      
      const spanToHighlight = spans[i] as HTMLElement;
      
      if (i === startSpan) {
        if (startWordIdx === 0) {
          highlightHtmlElement(spanToHighlight, highlightColor);
        } else {
          partialHighlight(startWordIdx, spanToHighlight, DIRECTION.START, highlightColor);
        }
      } else if (i === endSpan) {
        if (endWordIdx === 0) {
          return false;
        } else {
          partialHighlight(endWordIdx, spanToHighlight, DIRECTION.END, highlightColor);
        }
      } else {
        highlightHtmlElement(spanToHighlight, highlightColor);
      }
    }
    return true;
  }
  
  return false;
};

const HIGHLIGHT_CLASSNAME = "opacity-40 saturate-[3] highlighted-by-llama ";

const highlightHtmlElement = (div: HTMLElement, color: string) => {
  if (!div) return;
  
  const text = div.textContent || "";
  // Check if already highlighted
  if (div.querySelector(`.${HIGHLIGHT_CLASSNAME.trim()}`)) return;
  
  const newSpan = document.createElement("span");
  newSpan.className = HIGHLIGHT_CLASSNAME + color;
  newSpan.innerText = text;
  
  div.innerText = "";
  div.appendChild(newSpan);
};

enum DIRECTION {
  START,
  END,
}

const partialHighlight = (
  idx: number,
  span: HTMLElement,
  direction = DIRECTION.START,
  highlightColor: string
) => {
  if (!span) return;
  
  const text = span.textContent;
  if (!text) return;

  const words = text.split(" ");
  if (idx >= words.length) return;

  const substringToHighlight = words[idx - 1] || "";
  
  // Remove existing content
  span.textContent = "";
  
  if (direction === DIRECTION.START) {
    // First part normal, rest highlighted
    const normalText = document.createTextNode(words.slice(0, idx).join(" ") + " ");
    span.appendChild(normalText);
    
    const highlightSpan = document.createElement("span");
    highlightSpan.className = HIGHLIGHT_CLASSNAME + highlightColor;
    highlightSpan.textContent = words.slice(idx).join(" ");
    span.appendChild(highlightSpan);
  } else {
    // First part highlighted, rest normal
    const highlightSpan = document.createElement("span");
    highlightSpan.className = HIGHLIGHT_CLASSNAME + highlightColor;
    highlightSpan.textContent = words.slice(0, idx).join(" ") + " ";
    span.appendChild(highlightSpan);
    
    const normalText = document.createTextNode(words.slice(idx).join(" "));
    span.appendChild(normalText);
  }
};

// Your existing interface and functions with improved implementation
interface SearchStrings {
  text: string;
  startSpan: number;
  endSpan: number;
  startWordIdx: number;
  endWordIdx: number;
}

function generateDirectSearchData(
  startString: string,
  words: WordData[],
  n: number
): SearchStrings[] {
  const searchStrings: SearchStrings[] = [];
  const normalizedStartString = startString.toLowerCase().trim();
  
  for (let i = 0; i <= words.length - n; i++) {
    const currentWord = words[i]?.text || "";
    const normalizedCurrentWord = currentWord.toLowerCase().trim();
    
    // More flexible matching
    if (normalizedCurrentWord === normalizedStartString || 
        normalizedCurrentWord.includes(normalizedStartString) || 
        normalizedStartString.includes(normalizedCurrentWord)) {
      
      const text = words
        .slice(i, i + n)
        .map(val => val.text)
        .join(" ");
      
      searchStrings.push({
        text,
        startSpan: words[i]?.spanIdx || 0,
        endSpan: words[i + n - 1]?.spanIdx || 0,
        startWordIdx: words[i]?.wordIdx || 0,
        endWordIdx: words[i + n - 1]?.wordIdx || 0,
      });
    }
  }
  
  return searchStrings;
}

// The original generateFuzzySearchData function remains unchanged