import { useMemo } from 'react';

import CodeEditor from '@components/CodeEditor';
import { CodeBlockEditorProps } from '../constants';
import { EditorWrapperStyle } from './index.style';
import { isDebug } from '@utils/environment';

function Editor({
  autocompleteProviders,
  block,
  content,
  height,
  onChange,
  onContentSizeChangeCallback,
  onDidChangeCursorPosition,
  onMountCallback,
  placeholder,
  selected,
  setSelected,
  setTextareaFocused,
  shortcuts,
  textareaFocused,
}: CodeBlockEditorProps & {
  shortcuts?: ((monaco: any, editor: any) => void)[];
}) {
  if (isDebug()) {
    console.log('EditorV2 render', block?.uuid, Number(new Date()));
  }

  const {
    language,
  } = block;

  const editor = useMemo(() => (
    <CodeEditor
      autoHeight
      autocompleteProviders={autocompleteProviders}
      block={block}
      height={height}
      language={language}
      onChange={(val: string) => {
        onChange?.(val);
      }}
      onContentSizeChangeCallback={onContentSizeChangeCallback}
      onDidChangeCursorPosition={onDidChangeCursorPosition}
      onMountCallback={onMountCallback}
      placeholder={placeholder}
      selected={selected}
      setSelected={setSelected}
      setTextareaFocused={setTextareaFocused}
      shortcuts={shortcuts}
      textareaFocused={textareaFocused}
      value={content}
      width="100%"
    />
  ), [
    autocompleteProviders,
    block,
    content,
    height,
    language,
    onChange,
    onContentSizeChangeCallback,
    onDidChangeCursorPosition,
    onMountCallback,
    placeholder,
    selected,
    setSelected,
    setTextareaFocused,
    shortcuts,
    textareaFocused,
  ]);

  return (
    <EditorWrapperStyle>
      {editor}
    </EditorWrapperStyle>
  );
}

export default Editor;
